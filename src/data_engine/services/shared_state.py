"""Shared workspace snapshot and hydration services."""

from __future__ import annotations

from typing import Any

from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.runtime_db import RuntimeLedger
from data_engine.runtime.shared_state import hydrate_local_runtime_state, lease_is_stale, read_lease_metadata


class SharedStateService:
    """Own lease-based shared snapshot hydration for operator surfaces."""

    def hydrate_local_runtime(self, paths: WorkspacePaths, ledger: RuntimeLedger) -> None:
        """Replace one local runtime ledger from the shared workspace snapshots."""
        hydrate_local_runtime_state(paths, ledger)

    def read_lease_metadata(self, paths: WorkspacePaths) -> dict[str, Any] | None:
        """Return current workspace lease metadata, if present."""
        return read_lease_metadata(paths)

    def lease_is_stale(self, paths: WorkspacePaths, *, stale_after_seconds: float) -> bool:
        """Return whether current workspace lease metadata is stale."""
        return lease_is_stale(paths, stale_after_seconds=stale_after_seconds)


__all__ = ["SharedStateService"]
