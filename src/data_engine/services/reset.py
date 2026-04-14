"""Reset services for flow-scoped and workspace-scoped runtime state."""

from __future__ import annotations

from pathlib import Path

from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.services.runtime_ports import RuntimeCacheStore, RuntimeControlStore
from data_engine.services.shared_state import SharedStateService


def _clear_text_file_if_exists(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.write_text("", encoding="utf-8")
    except FileNotFoundError:
        pass
    except OSError:
        pass


class ResetService:
    """Own destructive reset operations for runtime history and workspace state."""

    def __init__(self, *, shared_state_service: SharedStateService | None = None) -> None:
        self.shared_state_service = shared_state_service or SharedStateService()

    def reset_flow(
        self,
        *,
        paths: WorkspacePaths,
        runtime_cache_ledger: RuntimeCacheStore,
        flow_name: str,
    ) -> None:
        """Delete one flow's persisted local and shared history/freshness state."""
        if hasattr(runtime_cache_ledger, "reset_flow"):
            runtime_cache_ledger.reset_flow(flow_name)
        self.shared_state_service.reset_flow_state(paths, flow_name=flow_name)

    def reset_workspace(
        self,
        *,
        paths: WorkspacePaths,
        runtime_cache_ledger: RuntimeCacheStore,
        runtime_control_ledger: RuntimeControlStore,
    ) -> None:
        """Delete all persisted local and shared runtime state for one workspace."""
        try:
            if hasattr(runtime_cache_ledger, "reset_all"):
                runtime_cache_ledger.reset_all()
            if hasattr(runtime_control_ledger, "reset_workspace"):
                runtime_control_ledger.reset_workspace(paths.workspace_id)
        finally:
            runtime_cache_ledger.close()
            runtime_control_ledger.close()
        self.shared_state_service.reset_workspace_state(paths)
        _clear_text_file_if_exists(paths.daemon_log_path)


__all__ = ["ResetService"]
