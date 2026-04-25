"""Mixin support methods for the GUI application shell."""

from __future__ import annotations

from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING

from PySide6.QtWidgets import QWidget

from data_engine.platform.workspace_models import authored_workspace_is_available
from data_engine.services import DaemonUpdateBatch
from data_engine.services.daemon_state import merge_update_batches
from data_engine.ui.gui.dialogs import show_message_box, structured_error_content
from data_engine.ui.gui.helpers import (
    is_last_process_ui_window as helper_is_last_process_ui_window,
)
from data_engine.ui.gui.presenters import (
    browse_dataframe_file as present_browse_dataframe_file,
    browse_dataframe_folder as present_browse_dataframe_folder,
    browse_workspace_collection_root_override as present_browse_workspace_collection_root_override,
    clear_dataframe_preview as present_clear_dataframe_preview,
    clear_workspace_debug_artifacts as present_clear_workspace_debug_artifacts,
    connect_dataframe_path as present_connect_dataframe_path,
    create_docs_browser as present_create_docs_browser,
    force_shutdown_daemon as present_force_shutdown_daemon,
    initialize_docs_view as present_initialize_docs_view,
    load_docs_page as present_load_docs_page,
    provision_selected_workspace as present_provision_selected_workspace,
    refresh_debug_artifacts as present_refresh_debug_artifacts,
    reset_workspace as present_reset_workspace,
    rebind_workspace_context as present_rebind_workspace_context,
    refresh_workspace_visibility_panel as present_refresh_workspace_visibility_panel,
    refresh_workspace_root_controls as present_refresh_workspace_root_controls,
    reset_workspace_collection_root_override as present_reset_workspace_collection_root_override,
    save_workspace_collection_root_override as present_save_workspace_collection_root_override,
    show_selected_dataframe_file as present_show_selected_dataframe_file,
    show_selected_debug_artifact as present_show_selected_debug_artifact,
)
from data_engine.ui.gui.surface import show_message_box_later as surface_show_message_box_later

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow
    from data_engine.views.models import QtFlowCard


class GuiWindowSupportMixin:
    """Shared window-support methods kept separate from the main GUI app shell."""

    _SUBSCRIPTION_HEALTH_WINDOW_SECONDS = 3.0

    def _workspace_binding_token(self: "DataEngineWindow") -> tuple[int, str]:
        """Return the current workspace-binding token for stale-result rejection."""
        return (
            int(getattr(self, "_workspace_binding_generation", 0)),
            str(self.workspace_paths.workspace_id),
        )

    def _advance_workspace_binding_generation(self: "DataEngineWindow") -> tuple[int, str]:
        """Advance and return the current workspace-binding token."""
        self._workspace_binding_generation = int(getattr(self, "_workspace_binding_generation", 0)) + 1
        return self._workspace_binding_token()

    def _matches_workspace_binding_token(
        self: "DataEngineWindow",
        token: tuple[int, str] | None,
    ) -> bool:
        """Return whether one worker/subscription token still matches the active binding."""
        return token == self._workspace_binding_token()

    def _control_action_payload(
        self: "DataEngineWindow",
        payload: object,
        *,
        token: tuple[int, str] | None = None,
    ) -> dict[str, object]:
        """Wrap one background control result with the current workspace token."""
        return {
            "workspace_token": self._workspace_binding_token() if token is None else token,
            "payload": payload,
        }

    def _unwrap_control_action_payload(
        self: "DataEngineWindow",
        payload: object,
    ) -> tuple[tuple[int, str] | None, object]:
        """Return the workspace token and inner control payload from one signal payload."""
        if isinstance(payload, dict) and "workspace_token" in payload and "payload" in payload:
            raw_token = payload.get("workspace_token")
            token = raw_token if isinstance(raw_token, tuple) and len(raw_token) == 2 else None
            return token, payload.get("payload")
        return None, payload

    def _daemon_batch_payload(
        self: "DataEngineWindow",
        batch: DaemonUpdateBatch,
        *,
        token: tuple[int, str] | None = None,
    ) -> dict[str, object]:
        """Wrap one daemon update batch with the current workspace token."""
        return {
            "workspace_token": self._workspace_binding_token() if token is None else token,
            "batch": batch,
        }

    def _unwrap_daemon_batch_payload(
        self: "DataEngineWindow",
        payload: object,
    ) -> tuple[tuple[int, str] | None, DaemonUpdateBatch | None]:
        """Return the workspace token and batch from one queued daemon payload."""
        if isinstance(payload, dict) and "workspace_token" in payload and "batch" in payload:
            raw_token = payload.get("workspace_token")
            token = raw_token if isinstance(raw_token, tuple) and len(raw_token) == 2 else None
            batch = payload.get("batch")
            return token, batch if isinstance(batch, DaemonUpdateBatch) else None
        if isinstance(payload, DaemonUpdateBatch):
            return None, payload
        return None, None

    def _resolve_workspace_paths(
        self: "DataEngineWindow",
        *,
        workspace_id: str | None = None,
        workspace_collection_root: Path | None | object = ...,
    ):
        """Resolve workspace paths using the current machine-local collection-root override."""
        if workspace_collection_root is ...:
            workspace_collection_root = self.workspace_collection_root_override
        return self.services.workspace_service.resolve_paths(
            workspace_id=workspace_id,
            workspace_collection_root=workspace_collection_root,
        )

    def _daemon_request(self: "DataEngineWindow", paths, payload, *, timeout: float = 0.0):
        """Send one request to the local workspace daemon."""
        return self.daemon_service.request(paths, payload, timeout=timeout)

    def _is_daemon_live(self: "DataEngineWindow", paths) -> bool:
        """Return whether the local workspace daemon is currently reachable."""
        return self.daemon_service.is_live(paths)

    def _daemon_client_error_type(self: "DataEngineWindow"):
        """Return the daemon client error type used by this module."""
        return self.daemon_service.client_error_type

    def _unregister_client_session_and_check_for_shutdown(
        self: "DataEngineWindow", *, purge_process_ui_sessions: bool
    ) -> bool:
        """Unregister this window's client session and report whether daemon shutdown is needed."""
        from data_engine.ui.gui.helpers import unregister_client_session_and_check_for_shutdown as helper_unregister

        return helper_unregister(self, purge_process_ui_sessions=purge_process_ui_sessions)

    def _is_last_process_ui_window(self: "DataEngineWindow") -> bool:
        """Return whether this is the last live GUI window in the current process."""
        return helper_is_last_process_ui_window(self)

    def _shutdown_daemon_on_close(self: "DataEngineWindow") -> None:
        """Shut down the daemon as part of final GUI process teardown."""
        from data_engine.ui.gui.helpers import shutdown_daemon_on_close as helper_shutdown

        helper_shutdown(self)

    def _wait_for_worker_threads(self: "DataEngineWindow", *, timeout_seconds: float) -> None:
        """Join outstanding GUI worker threads during shutdown."""
        from data_engine.ui.gui.helpers import wait_for_worker_threads as helper_wait

        helper_wait(self, timeout_seconds=timeout_seconds)

    def _register_worker_thread(self: "DataEngineWindow", thread) -> None:
        """Track one GUI worker thread under the window-local lock."""
        if not hasattr(self, "_worker_threads_lock") or not hasattr(self, "_worker_threads"):
            return
        with self._worker_threads_lock:
            self._worker_threads.add(thread)

    def _discard_worker_thread(self: "DataEngineWindow", thread) -> None:
        """Stop tracking one GUI worker thread under the window-local lock."""
        if not hasattr(self, "_worker_threads_lock") or not hasattr(self, "_worker_threads"):
            return
        with self._worker_threads_lock:
            self._worker_threads.discard(thread)

    def _worker_threads_snapshot(self: "DataEngineWindow") -> tuple:
        """Return a stable snapshot of tracked GUI worker threads."""
        if not hasattr(self, "_worker_threads_lock") or not hasattr(self, "_worker_threads"):
            return ()
        with self._worker_threads_lock:
            return tuple(self._worker_threads)

    def _schedule_daemon_update_sync(self: "DataEngineWindow") -> None:
        """Queue one daemon-driven full sync back onto the Qt main thread."""
        if getattr(self, "ui_closing", False):
            return
        self.daemon_subscription.mark_subscription(self._monotonic())
        signals = getattr(self, "signals", None)
        if signals is None:
            return
        try:
            signals.daemon_update_available.emit()
        except RuntimeError:
            return

    def _schedule_daemon_update_batch(self: "DataEngineWindow", batch: DaemonUpdateBatch) -> None:
        """Queue one lane-based daemon update back onto the Qt main thread."""
        if getattr(self, "ui_closing", False):
            return
        self.daemon_subscription.mark_subscription(self._monotonic())
        token = self._workspace_binding_token()
        existing_token, existing_batch = self._unwrap_daemon_batch_payload(
            getattr(self, "_pending_daemon_update_batch", None)
        )
        merged_batch = (
            merge_update_batches(existing_batch, batch)
            if existing_batch is not None and existing_token == token
            else batch
        )
        self._pending_daemon_update_batch = self._daemon_batch_payload(merged_batch, token=token)
        signals = getattr(self, "signals", None)
        if signals is None:
            return
        try:
            signals.daemon_update_batch_available.emit(self._daemon_batch_payload(batch, token=token))
        except RuntimeError:
            return

    def _ensure_daemon_wait_worker(self: "DataEngineWindow") -> None:
        """Ensure the daemon wait worker is running for the current workspace."""
        if getattr(self, "ui_closing", False):
            return
        if not self._has_authored_workspace():
            return
        from data_engine.ui.gui.helpers import start_worker_thread

        self.daemon_subscription.ensure_started(
            workspace_available=lambda: not self.ui_closing and self._has_authored_workspace(),
            on_update=lambda batch: (None if self.ui_closing else self._schedule_daemon_update_batch(batch)),
            start_worker=lambda target: start_worker_thread(self, target=target),
        )

    def _should_run_daemon_heartbeat(self: "DataEngineWindow") -> bool:
        """Return whether the heartbeat path should perform a sync right now."""
        return self.daemon_subscription.should_run_heartbeat(getattr(self, "workspace_snapshot", None))

    def _heartbeat_daemon_sync(self: "DataEngineWindow") -> None:
        """Run a low-frequency daemon heartbeat when the subscription path is degraded."""
        self._ensure_daemon_wait_worker()
        if not self._should_run_daemon_heartbeat():
            return
        self._sync_from_daemon()

    def _switch_view(self: "DataEngineWindow", index: int) -> None:
        self.view_stack.setCurrentIndex(index)
        if index == 2:
            self._refresh_debug_artifacts()
        if hasattr(self, "workspace_counts_footer_label"):
            self.workspace_counts_footer_label.setVisible(index == 0)
        if hasattr(self, "app_version_footer_label"):
            self.app_version_footer_label.setVisible(index == 0)

    def _monotonic(self: "DataEngineWindow") -> float:
        """Return the current monotonic clock value."""
        return monotonic()

    def _structured_error_content(self: "DataEngineWindow", text: str):
        """Parse one developer-facing flow-module error into dialog sections when possible."""
        return structured_error_content(text)

    def _show_message_box(self: "DataEngineWindow", *, title: str, text: str, tone: str) -> None:
        """Show one simple application dialog for info/error messages."""
        show_message_box(self, title=title, text=text, tone=tone)

    def _show_message_box_later(self: "DataEngineWindow", *, title: str, text: str, tone: str) -> None:
        """Defer one application dialog until the current UI update cycle completes."""
        surface_show_message_box_later(self, title=title, text=text, tone=tone)

    def _refresh_workspace_root_controls(self: "DataEngineWindow") -> None:
        """Refresh local workspace-root override copy in the Settings view."""
        present_refresh_workspace_root_controls(self)

    def _rebind_workspace_context(self: "DataEngineWindow", *, workspace_id: str | None = None) -> None:
        """Re-resolve paths and rebuild local UI state after workspace settings change."""
        present_rebind_workspace_context(self, workspace_id=workspace_id)

    def _save_workspace_collection_root_override(self: "DataEngineWindow") -> None:
        """Persist one machine-local workspace collection root override."""
        present_save_workspace_collection_root_override(self)

    def _reset_workspace_collection_root_override(self: "DataEngineWindow") -> None:
        """Clear the machine-local workspace collection root override."""
        present_reset_workspace_collection_root_override(self)

    def _browse_workspace_collection_root_override(self: "DataEngineWindow") -> None:
        """Open a folder picker for the local workspace collection root override."""
        present_browse_workspace_collection_root_override(self)

    def _browse_dataframe_file(self: "DataEngineWindow") -> None:
        """Open a parquet-file picker for the Dataframes view."""
        present_browse_dataframe_file(self)

    def _browse_dataframe_folder(self: "DataEngineWindow") -> None:
        """Open a parquet-folder picker for the Dataframes view."""
        present_browse_dataframe_folder(self)

    def _connect_dataframe_path(self: "DataEngineWindow", path: Path) -> None:
        """Connect one parquet file or folder to the Dataframes view."""
        present_connect_dataframe_path(self, path)

    def _show_selected_dataframe_file(self: "DataEngineWindow") -> None:
        """Render the selected Dataframes view parquet file."""
        present_show_selected_dataframe_file(self)

    def _clear_dataframe_preview(self: "DataEngineWindow", message: str) -> None:
        """Clear the Dataframes view preview pane."""
        present_clear_dataframe_preview(self, message)

    def _provision_selected_workspace(self: "DataEngineWindow") -> None:
        """Provision the selected authored workspace without overwriting existing files."""
        present_provision_selected_workspace(self)

    def _force_shutdown_daemon(self: "DataEngineWindow") -> None:
        """Force-stop the local workspace daemon for the selected workspace."""
        present_force_shutdown_daemon(self)

    def _reset_workspace(self: "DataEngineWindow") -> None:
        """Delete local and shared runtime state for the selected workspace."""
        present_reset_workspace(self)

    def _refresh_workspace_visibility_panel(self: "DataEngineWindow") -> None:
        """Refresh the read-only workspace visibility stats shown in Settings."""
        present_refresh_workspace_visibility_panel(self)

    def _refresh_debug_artifacts(self: "DataEngineWindow") -> None:
        """Refresh the Debug view artifact selector for the current workspace."""
        present_refresh_debug_artifacts(self)

    def _show_selected_debug_artifact(self: "DataEngineWindow") -> None:
        """Render the currently selected Debug artifact."""
        present_show_selected_debug_artifact(self)

    def _clear_debug_artifacts(self: "DataEngineWindow") -> None:
        """Delete all saved Debug artifacts for the current workspace."""
        present_clear_workspace_debug_artifacts(self)

    def _create_docs_browser(self: "DataEngineWindow") -> QWidget:
        return present_create_docs_browser(self)

    def _initialize_docs_view(self: "DataEngineWindow") -> None:
        present_initialize_docs_view(self)

    def _load_docs_page(self: "DataEngineWindow", file_name: str) -> None:
        present_load_docs_page(self, file_name)

    def _config_summary(self: "DataEngineWindow", card: "QtFlowCard | None"):
        from data_engine.domain import ConfigPreviewState

        return ConfigPreviewState.from_flow(card, self.flow_states)

    def _has_authored_workspace(self: "DataEngineWindow") -> bool:
        """Return whether the selected workspace currently has authored flow modules."""
        return authored_workspace_is_available(self.workspace_paths)

    def _is_bootstrap_ready_error(self: "DataEngineWindow", message: str) -> bool:
        """Return whether one flow-load error is expected before bootstrap."""
        return "No flow modules discovered" in message or ("Flow module" in message and "is not available" in message)

    def _empty_flow_message_for_error(self: "DataEngineWindow", message: str) -> str:
        """Return the UI empty-state copy for one flow-load failure."""
        if self._is_bootstrap_ready_error(message):
            return "No discoverable flows were found yet in this workspace folder."
        return message
