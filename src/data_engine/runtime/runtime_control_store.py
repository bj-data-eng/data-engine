"""SQLite-backed runtime control store for daemon ownership and client sessions."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Self

from data_engine.domain.time import utcnow_text
from data_engine.platform.processes import process_is_running
from data_engine.platform.workspace_models import DATA_ENGINE_RUNTIME_CONTROL_DB_PATH_ENV_VAR
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.ledger_models import PersistedDaemonState
from data_engine.runtime.sqlite_store import _RuntimeSqliteStore


class DaemonStateRepository:
    """Repository for persisted daemon ownership metadata."""

    def __init__(self, store: _RuntimeSqliteStore) -> None:
        self._store = store

    def upsert(
        self,
        *,
        workspace_id: str,
        pid: int,
        endpoint_kind: str,
        endpoint_path: str,
        started_at_utc: str,
        last_checkpoint_at_utc: str,
        status: str,
        app_root: str,
        workspace_root: str,
        version_text: str | None = None,
    ) -> None:
        self._store._connection().execute(
            """
            INSERT INTO daemon_state(
                workspace_id,
                pid,
                endpoint_kind,
                endpoint_path,
                started_at_utc,
                last_checkpoint_at_utc,
                status,
                app_root,
                workspace_root,
                version_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(workspace_id) DO UPDATE SET
                pid = excluded.pid,
                endpoint_kind = excluded.endpoint_kind,
                endpoint_path = excluded.endpoint_path,
                started_at_utc = excluded.started_at_utc,
                last_checkpoint_at_utc = excluded.last_checkpoint_at_utc,
                status = excluded.status,
                app_root = excluded.app_root,
                workspace_root = excluded.workspace_root,
                version_text = excluded.version_text
            """,
            (
                workspace_id,
                pid,
                endpoint_kind,
                endpoint_path,
                started_at_utc,
                last_checkpoint_at_utc,
                status,
                app_root,
                workspace_root,
                version_text,
            ),
        )

    def get(self, workspace_id: str) -> PersistedDaemonState | None:
        row = self._store._connection().execute(
            """
            SELECT workspace_id, pid, endpoint_kind, endpoint_path, started_at_utc, last_checkpoint_at_utc, status, app_root, workspace_root, version_text
            FROM daemon_state
            WHERE workspace_id = ?
            """,
            (workspace_id,),
        ).fetchone()
        if row is None:
            return None
        return PersistedDaemonState(
            workspace_id=str(row["workspace_id"]),
            pid=int(row["pid"]),
            endpoint_kind=str(row["endpoint_kind"]),
            endpoint_path=str(row["endpoint_path"]),
            started_at_utc=str(row["started_at_utc"]),
            last_checkpoint_at_utc=str(row["last_checkpoint_at_utc"]),
            status=str(row["status"]),
            app_root=str(row["app_root"]),
            workspace_root=str(row["workspace_root"]),
            version_text=row["version_text"],
        )

    def clear(self, workspace_id: str) -> None:
        self._store._connection().execute("DELETE FROM daemon_state WHERE workspace_id = ?", (workspace_id,))


class ClientSessionRepository:
    """Repository for persisted local UI/client sessions."""

    def __init__(self, store: _RuntimeSqliteStore) -> None:
        self._store = store

    def upsert(self, *, client_id: str, workspace_id: str, client_kind: str, pid: int) -> None:
        row = self._store._connection().execute(
            "SELECT started_at_utc FROM client_sessions WHERE client_id = ?",
            (client_id,),
        ).fetchone()
        started_at_utc = str(row["started_at_utc"]) if row is not None and row["started_at_utc"] else ""
        if not started_at_utc:
            started_at_utc = utcnow_text()
        updated_at_utc = started_at_utc if row is None else utcnow_text()
        self._store._connection().execute(
            """
            INSERT INTO client_sessions(
                client_id,
                workspace_id,
                client_kind,
                pid,
                started_at_utc,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(client_id) DO UPDATE SET
                workspace_id = excluded.workspace_id,
                client_kind = excluded.client_kind,
                pid = excluded.pid,
                updated_at_utc = excluded.updated_at_utc
            """,
            (client_id, workspace_id, client_kind, pid, started_at_utc, updated_at_utc),
        )

    def remove(self, client_id: str) -> None:
        self._store._connection().execute("DELETE FROM client_sessions WHERE client_id = ?", (client_id,))

    def remove_for_process(self, *, workspace_id: str, client_kind: str, pid: int) -> None:
        self._store._connection().execute(
            """
            DELETE FROM client_sessions
            WHERE workspace_id = ?
              AND client_kind = ?
              AND pid = ?
            """,
            (workspace_id, client_kind, pid),
        )

    def count_live(self, workspace_id: str, *, exclude_client_id: str | None = None) -> int:
        rows = self._store._connection().execute(
            """
            SELECT client_id, pid
            FROM client_sessions
            WHERE workspace_id = ?
            """,
            (workspace_id,),
        ).fetchall()
        live_count = 0
        stale_client_ids: list[str] = []
        for row in rows:
            client_id = str(row["client_id"])
            if exclude_client_id is not None and client_id == exclude_client_id:
                continue
            pid = int(row["pid"])
            if process_is_running(pid, treat_defunct_as_dead=False):
                live_count += 1
            else:
                stale_client_ids.append(client_id)
        if stale_client_ids:
            self._store._connection().executemany(
                "DELETE FROM client_sessions WHERE client_id = ?",
                ((client_id,) for client_id in stale_client_ids),
            )
        return live_count


class RuntimeControlLedger(_RuntimeSqliteStore):
    """Own the control SQLite store for daemon ownership and client sessions."""

    def __init__(self, db_path: Path) -> None:
        super().__init__(db_path)
        self.daemon_state = DaemonStateRepository(self)
        self.client_sessions = ClientSessionRepository(self)

    @classmethod
    def open_default(cls, *, data_root: Path | None = None) -> Self:
        env_override_raw = os.environ.get(DATA_ENGINE_RUNTIME_CONTROL_DB_PATH_ENV_VAR)
        if env_override_raw is not None and env_override_raw.strip():
            return cls(Path(env_override_raw).expanduser().resolve())
        return cls(RuntimeLayoutPolicy().resolve_paths(data_root=data_root).runtime_control_db_path)

    def _initialize_schema(self) -> None:
        connection = self._connection()
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS daemon_state (
                workspace_id TEXT PRIMARY KEY,
                pid INTEGER NOT NULL,
                endpoint_kind TEXT NOT NULL,
                endpoint_path TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                last_checkpoint_at_utc TEXT NOT NULL,
                status TEXT NOT NULL,
                app_root TEXT NOT NULL,
                workspace_root TEXT NOT NULL,
                version_text TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS client_sessions (
                client_id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                client_kind TEXT NOT NULL,
                pid INTEGER NOT NULL,
                started_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        connection.execute("CREATE INDEX IF NOT EXISTS idx_client_sessions_workspace ON client_sessions(workspace_id, updated_at_utc DESC)")
        self._checkpoint_wal(passive=True)


__all__ = ["ClientSessionRepository", "DaemonStateRepository", "RuntimeControlLedger"]
