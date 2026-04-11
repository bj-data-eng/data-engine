"""Runtime control-ledger services."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from data_engine.platform.paths import stable_absolute_path
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.runtime_db import RuntimeControlLedger


class RuntimeControlLedgerService:
    """Own workspace-local runtime control-ledger access and client-session bookkeeping."""

    def __init__(
        self,
        open_ledger_func: Callable[[Path], RuntimeControlLedger] | None = None,
        *,
        runtime_layout_policy: RuntimeLayoutPolicy | None = None,
    ) -> None:
        self.runtime_layout_policy = runtime_layout_policy or RuntimeLayoutPolicy()
        self._open_ledger_func = open_ledger_func or self._open_default_ledger

    def _open_default_ledger(self, workspace_root: Path) -> RuntimeControlLedger:
        paths = self.runtime_layout_policy.resolve_paths(workspace_root=workspace_root)
        return RuntimeControlLedger(paths.runtime_control_db_path)

    def open_for_workspace(self, workspace_root: Path) -> RuntimeControlLedger:
        """Open the configured runtime control ledger for one workspace root."""
        return self._open_ledger_func(stable_absolute_path(workspace_root))

    def close(self, ledger: RuntimeControlLedger) -> None:
        """Close one runtime control-ledger connection."""
        ledger.close()

    def register_client_session(
        self,
        ledger: RuntimeControlLedger,
        *,
        client_id: str,
        workspace_id: str,
        client_kind: str,
        pid: int,
    ) -> None:
        """Register or refresh one active local client session."""
        ledger.client_sessions.upsert(
            client_id=client_id,
            workspace_id=workspace_id,
            client_kind=client_kind,
            pid=pid,
        )

    def remove_client_session(self, ledger: RuntimeControlLedger, client_id: str) -> None:
        """Remove one active local client session row."""
        ledger.client_sessions.remove(client_id)

    def purge_process_client_sessions(
        self,
        ledger: RuntimeControlLedger,
        *,
        workspace_id: str,
        client_kind: str,
        pid: int,
    ) -> None:
        """Remove all client sessions for one workspace/client-kind/process tuple."""
        ledger.client_sessions.remove_for_process(
            workspace_id=workspace_id,
            client_kind=client_kind,
            pid=pid,
        )

    def count_live_client_sessions(
        self,
        ledger: RuntimeControlLedger,
        workspace_id: str,
        *,
        exclude_client_id: str | None = None,
    ) -> int:
        """Return the number of currently live client sessions for one workspace."""
        if exclude_client_id is None:
            return ledger.client_sessions.count_live(workspace_id)
        return ledger.client_sessions.count_live(workspace_id, exclude_client_id=exclude_client_id)


LedgerService = RuntimeControlLedgerService


__all__ = ["LedgerService", "RuntimeControlLedgerService"]
