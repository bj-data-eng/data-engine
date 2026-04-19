"""Live home-view state provider for the experimental egui surface."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from threading import RLock, Thread
import time
from typing import Any
from uuid import uuid4

from data_engine.domain import PendingWorkspaceActionOverlay
from data_engine.platform.workspace_models import DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR
from data_engine.services import (
    DaemonUpdateBatch,
    DaemonUpdateSubscription,
    flow_state_texts_from_workspace_snapshot,
    runtime_session_from_workspace_snapshot,
)
from data_engine.ui.egui.bootstrap import EguiServices, build_egui_services, default_egui_service_kwargs
from data_engine.views import (
    GuiActionState,
    RunGroupDisplay,
    build_operator_action_context,
    build_selected_flow_presentation,
    flow_secondary_text,
    format_seconds,
    group_cards,
    group_secondary_text,
)

_ACTIVE_FLOW_STATES = {"running", "polling", "scheduled", "stopping flow", "stopping runtime"}


@dataclass(frozen=True)
class _ProjectionBundle:
    workspace_ids: tuple[str, ...]
    workspace_id: str
    workspace_root: str
    flow_payload: dict[str, Any]


class EguiHomeStateProvider:
    """Maintain one stream-fed home-view model for the Rust egui shell."""

    def __init__(self, *, title: str | None = None) -> None:
        del title
        self._lock = RLock()
        self._closed = False
        self._services: EguiServices = build_egui_services(**default_egui_service_kwargs("system"))
        self._client_session_id = uuid4().hex
        self._workspace_collection_root_override = self._initial_workspace_override()
        self._workspace_paths = self._services.workspace_service.resolve_paths(
            workspace_collection_root=self._workspace_collection_root_override,
        )
        self._binding = self._services.runtime_binding_service.open_binding(self._workspace_paths)
        self._services.runtime_binding_service.register_client_session(
            self._binding,
            client_id=self._client_session_id,
            client_kind="egui",
        )
        self._daemon_subscription = DaemonUpdateSubscription(
            daemon_state_service=self._services.daemon_state_service,
            manager=self._binding.daemon_manager,
            clock=time.monotonic,
        )
        self._state_json = json.dumps(self._empty_state())
        self._rebuild_state()
        self._ensure_subscription()

    def snapshot_json(self) -> str:
        """Return the latest rendered home-state payload as JSON."""
        with self._lock:
            return self._state_json

    def refresh(self) -> None:
        """Force an immediate state rebuild from the current workspace."""
        with self._lock:
            if self._closed:
                return
        self._rebuild_state()

    def select_workspace(self, workspace_id: str) -> None:
        """Switch the provider to a different discovered workspace."""
        candidate = str(workspace_id or "").strip()
        if not candidate:
            return
        with self._lock:
            if self._closed or candidate == self._workspace_paths.workspace_id:
                return
            self._rebind_workspace(candidate)
        self._rebuild_state()
        self._ensure_subscription()

    def close(self) -> None:
        """Stop streaming updates and release workspace bindings."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            subscription = self._daemon_subscription
            binding = self._binding
        subscription.stop()
        self._services.runtime_binding_service.remove_client_session(binding, self._client_session_id)
        self._services.runtime_binding_service.close_binding(binding)

    def _initial_workspace_override(self) -> Path | None:
        env_collection_root = os.environ.get(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR)
        if env_collection_root and env_collection_root.strip():
            return None
        return self._services.settings_service.workspace_collection_root()

    def _discovered_workspace_ids(self) -> tuple[str, ...]:
        discovered = self._services.workspace_service.discover(
            app_root=self._workspace_paths.app_root,
            workspace_collection_root=self._workspace_collection_root_override,
        )
        return tuple(item.workspace_id for item in discovered)

    def _rebind_workspace(self, workspace_id: str) -> None:
        self._daemon_subscription.stop()
        self._services.runtime_binding_service.remove_client_session(self._binding, self._client_session_id)
        self._services.runtime_binding_service.close_binding(self._binding)
        self._workspace_paths = self._services.workspace_service.resolve_paths(
            workspace_id=workspace_id,
            workspace_collection_root=self._workspace_collection_root_override,
        )
        self._services.settings_service.set_default_workspace_id(self._workspace_paths.workspace_id)
        self._binding = self._services.runtime_binding_service.open_binding(self._workspace_paths)
        self._services.runtime_binding_service.register_client_session(
            self._binding,
            client_id=self._client_session_id,
            client_kind="egui",
        )
        self._daemon_subscription = DaemonUpdateSubscription(
            daemon_state_service=self._services.daemon_state_service,
            manager=self._binding.daemon_manager,
            clock=time.monotonic,
        )

    def _start_worker(self, target) -> Thread:
        thread = Thread(target=target, daemon=True, name=f"egui-daemon:{self._workspace_paths.workspace_id}")
        thread.start()
        return thread

    def _ensure_subscription(self) -> None:
        self._daemon_subscription.ensure_started(
            workspace_available=lambda: not self._closed and self._workspace_paths.workspace_configured,
            on_update=self._on_daemon_update,
            start_worker=self._start_worker,
        )

    def _on_daemon_update(self, batch: DaemonUpdateBatch) -> None:
        if self._closed:
            return
        self._daemon_subscription.mark_subscription()
        self._rebuild_state()

    def _rebuild_state(self) -> None:
        with self._lock:
            if self._closed:
                return
            flow_payload = self._build_flow_payload()
            bundle = _ProjectionBundle(
                workspace_ids=self._discovered_workspace_ids(),
                workspace_id=self._workspace_paths.workspace_id,
                workspace_root=str(self._workspace_paths.workspace_root),
                flow_payload=flow_payload,
            )
            self._state_json = json.dumps(
                {
                    "workspace_ids": list(bundle.workspace_ids),
                    "selected_workspace_id": bundle.workspace_id,
                    "workspace_root": bundle.workspace_root,
                    **bundle.flow_payload,
                },
                sort_keys=True,
            )

    def _build_flow_payload(self) -> dict[str, Any]:
        catalog_result = self._services.catalog_query_service.load_workspace_catalog(
            workspace_root=self._workspace_paths.workspace_root,
            missing_message=(
                "Workspace collection root is not configured."
                if not self._workspace_paths.workspace_configured
                else "No flow modules discovered."
            ),
        )
        catalog_state = catalog_result.catalog_state
        catalog_presentation = self._services.catalog_query_service.build_catalog_presentation(
            catalog_state=catalog_state,
        )
        flow_cards = catalog_presentation.entries
        now = time.monotonic()
        sync_state = self._services.runtime_binding_service.sync_runtime_state(
            self._binding,
            runtime_application=self._services.runtime_application,
            flow_cards=flow_cards,
        )
        projection = self._services.runtime_state_service.rebuild_projection(
            self._binding,
            runtime_application=self._services.runtime_application,
            flow_cards=flow_cards,
            runtime_session=sync_state.runtime_session,
            now=now,
        )
        snapshot = self._services.runtime_state_service.snapshot_from_projection(
            binding=self._binding,
            flow_cards=flow_cards,
            projection=projection,
            workspace_control_state=sync_state.workspace_control_state,
            daemon_live=bool(getattr(sync_state.snapshot, "live", False)),
            daemon_projection_version=int(getattr(sync_state.snapshot, "projection_version", 0) or 0),
            daemon_transport_mode=str(getattr(sync_state.snapshot, "transport_mode", "heartbeat") or "heartbeat"),
            daemon_engine_starting=bool(getattr(sync_state.snapshot, "engine_starting", False)),
            daemon_active_flow_names=tuple(getattr(sync_state.snapshot, "active_engine_flow_names", ()) or ()),
            daemon_active_runs=tuple(getattr(sync_state.snapshot, "active_runs", ()) or ()),
            daemon_flow_activity=tuple(getattr(sync_state.snapshot, "flow_activity", ()) or ()),
        )
        flow_states = flow_state_texts_from_workspace_snapshot(snapshot, flow_cards)
        effective_runtime_session = runtime_session_from_workspace_snapshot(snapshot)
        grouped_cards = group_cards(flow_cards)
        flow_groups = []
        flow_detail_map: dict[str, Any] = {}
        has_automated_flows = any(card.valid and card.mode in {"poll", "schedule"} for card in flow_cards)
        for bucket in grouped_cards:
            entries = list(bucket.entries)
            flow_groups.append(
                {
                    "group_name": bucket.group_name,
                    "title": bucket.title,
                    "secondary": group_secondary_text(entries, flow_states),
                    "flows": [
                        {
                            "flow_name": card.name,
                            "title": card.title,
                            "secondary": flow_secondary_text(card.mode, flow_states.get(card.name, card.state)),
                            "state": flow_states.get(card.name, card.state),
                            "group_name": card.group or "",
                            "valid": bool(card.valid),
                        }
                        for card in entries
                    ],
                }
            )
        flow_groups_by_name = {card.name: card.group for card in flow_cards}
        for card in flow_cards:
            run_groups = self._services.history_query_service.list_flow_runs(
                self._binding.log_store,
                flow_name=card.name,
            )
            selected_presentation = build_selected_flow_presentation(
                card=card,
                tracker=projection.operation_tracker,
                flow_states=flow_states,
                run_groups=run_groups,
                selected_run_key=None,
                max_visible_runs=50,
                live_runs=snapshot.active_runs,
                live_truth_authoritative=snapshot.engine.daemon_live,
            )
            action_context = build_operator_action_context(
                card=card,
                flow_states=flow_states,
                runtime_session=effective_runtime_session,
                flow_groups_by_name=flow_groups_by_name,
                active_flow_states=_ACTIVE_FLOW_STATES,
                engine_state=snapshot.engine.state,
                engine_truth_known=True,
                live_runs=snapshot.active_runs,
                engine_active_flow_names=snapshot.engine.active_flow_names,
                has_logs=bool(selected_presentation.run_groups),
                has_automated_flows=has_automated_flows,
                workspace_available=self._workspace_paths.workspace_configured,
                selected_run_group_present=selected_presentation.selected_run_group is not None,
                overlay=PendingWorkspaceActionOverlay(),
            )
            action_state = GuiActionState.from_context(action_context)
            detail_state = selected_presentation.detail_state
            summary_rows = [] if detail_state is None else [
                {"label": row.label, "value": row.value} for row in detail_state.summary_rows
            ]
            steps = [] if detail_state is None else [
                {
                    "number": index,
                    "title": row.name,
                    "status": row.status,
                    "duration": self._step_duration_text(row),
                    "inspectable": bool(row.name),
                    "active_count": row.active_count,
                }
                for index, row in enumerate(detail_state.operation_rows, start=1)
            ]
            logs = [
                {
                    "timestamp": display.primary_label,
                    "label": self._run_group_label(run_group, display),
                    "duration": display.duration_text or "",
                    "state": display.status_text,
                    "inspectable": display.status_visual_state == "failed",
                }
                for run_group in selected_presentation.visible_run_groups
                for display in (RunGroupDisplay.from_run(run_group),)
            ]
            flow_detail_map[card.name] = {
                "flow_name": card.name,
                "title": card.title,
                "description": card.description,
                "error": card.error,
                "group_name": card.group or "",
                "summary_rows": summary_rows,
                "steps": steps,
                "logs": logs,
                "actions": {
                    "flow_run_label": action_state.flow_run_label,
                    "flow_run_enabled": action_state.flow_run_enabled,
                    "flow_config_enabled": action_state.flow_config_enabled,
                },
            }
        engine_context = build_operator_action_context(
            card=catalog_presentation.selected_card,
            flow_states=flow_states,
            runtime_session=effective_runtime_session,
            flow_groups_by_name=flow_groups_by_name,
            active_flow_states=_ACTIVE_FLOW_STATES,
            engine_state=snapshot.engine.state,
            engine_truth_known=True,
            live_runs=snapshot.active_runs,
            engine_active_flow_names=snapshot.engine.active_flow_names,
            has_logs=bool(catalog_presentation.selected_card and flow_detail_map.get(catalog_presentation.selected_card.name, {}).get("logs")),
            has_automated_flows=has_automated_flows,
            workspace_available=self._workspace_paths.workspace_configured,
            selected_run_group_present=False,
            overlay=PendingWorkspaceActionOverlay(),
        )
        engine_action_state = GuiActionState.from_context(engine_context)
        return {
            "empty_message": catalog_state.empty_message,
            "engine": {
                "label": engine_action_state.engine_label,
                "enabled": engine_action_state.engine_enabled,
                "state": engine_action_state.engine_state,
            },
            "request_control": {
                "label": engine_action_state.request_control_label,
                "enabled": engine_action_state.request_control_enabled,
                "visible": engine_action_state.request_control_visible,
            },
            "refresh": {
                "enabled": engine_action_state.refresh_enabled,
            },
            "flow_groups": flow_groups,
            "flows": flow_detail_map,
            "default_selected_flow_name": catalog_presentation.selected_flow_name,
        }

    @staticmethod
    def _step_duration_text(row) -> str:
        if row.active_count > 1:
            return f"{row.active_count} active"
        if row.live_elapsed_seconds is not None:
            return format_seconds(row.live_elapsed_seconds)
        if row.elapsed_seconds is not None:
            return format_seconds(row.elapsed_seconds)
        return ""

    @staticmethod
    def _run_group_label(run_group, display: RunGroupDisplay) -> str:
        has_source = run_group.source_label not in {"", "-"}
        if has_source:
            return f"{run_group.key[0]} > {run_group.source_label}"
        return run_group.key[0]

    @staticmethod
    def _empty_state() -> dict[str, object]:
        return {
            "workspace_ids": [],
            "selected_workspace_id": "",
            "workspace_root": "",
            "empty_message": "No workspace selected.",
            "engine": {"label": "Start Engine", "enabled": False, "state": "stopped"},
            "request_control": {"label": "Request Control", "enabled": False, "visible": True},
            "refresh": {"enabled": False},
            "flow_groups": [],
            "flows": {},
            "default_selected_flow_name": None,
        }


__all__ = ["EguiHomeStateProvider"]
