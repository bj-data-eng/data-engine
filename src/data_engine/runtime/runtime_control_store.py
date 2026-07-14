"""SQLite-backed runtime control store for daemon ownership and client sessions."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Self

from data_engine.domain.time import utcnow_text
from data_engine.platform.paths import normalized_path_text, stable_path_identity_text
from data_engine.platform.processes import ProcessIdentity, process_is_running
from data_engine.platform.workspace_models import DATA_ENGINE_RUNTIME_CONTROL_DB_PATH_ENV_VAR
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.ledger_models import PersistedDaemonState
from data_engine.runtime.sqlite_store import _RuntimeSqliteStore


def _binding_path_matches(actual: str, expected: str) -> bool:
    return stable_path_identity_text(actual) == stable_path_identity_text(expected)


def _endpoint_matches(kind: str, actual: str, expected: str) -> bool:
    if kind == "pipe":
        return normalized_path_text(actual).casefold() == normalized_path_text(
            expected
        ).casefold()
    return _binding_path_matches(actual, expected)


def _daemon_state_binding_matches(
    state: PersistedDaemonState | None,
    *,
    workspace_id: str,
    endpoint_kind: str,
    endpoint_path: str,
    app_root: str,
    workspace_root: str,
) -> bool:
    return bool(
        state is not None
        and state.workspace_id == workspace_id
        and state.endpoint_kind == endpoint_kind
        and _endpoint_matches(endpoint_kind, state.endpoint_path, endpoint_path)
        and _binding_path_matches(state.app_root, app_root)
        and _binding_path_matches(state.workspace_root, workspace_root)
    )


def _daemon_state_generation_matches(
    state: PersistedDaemonState | None,
    *,
    process_identity: ProcessIdentity | None,
    containment_nonce: str | None,
) -> bool:
    return bool(
        state is not None
        and process_identity is not None
        and containment_nonce is not None
        and state.pid == process_identity.pid
        and state.process_start_key == process_identity.start_key
        and state.process_executable_path == process_identity.executable_path
        and state.process_group_id == process_identity.process_group_id
        and state.process_session_id == process_identity.process_session_id
        and state.containment_nonce == containment_nonce
    )


class DaemonStateRepository:
    """Repository for persisted daemon ownership metadata."""

    def __init__(self, store: _RuntimeSqliteStore) -> None:
        self._store = store

    def upsert(
        self,
        *,
        workspace_id: str,
        daemon_id: str,
        pid: int,
        process_start_key: str,
        process_executable_path: str,
        process_group_id: int | None,
        process_session_id: int | None,
        containment_nonce: str,
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
                daemon_id,
                pid,
                process_start_key,
                process_executable_path,
                process_group_id,
                process_session_id,
                containment_nonce,
                endpoint_kind,
                endpoint_path,
                started_at_utc,
                last_checkpoint_at_utc,
                status,
                app_root,
                workspace_root,
                version_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(workspace_id) DO UPDATE SET
                daemon_id = excluded.daemon_id,
                pid = excluded.pid,
                process_start_key = excluded.process_start_key,
                process_executable_path = excluded.process_executable_path,
                process_group_id = excluded.process_group_id,
                process_session_id = excluded.process_session_id,
                containment_nonce = excluded.containment_nonce,
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
                daemon_id,
                pid,
                process_start_key,
                process_executable_path,
                process_group_id,
                process_session_id,
                containment_nonce,
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

    def install_provisional(
        self,
        *,
        workspace_id: str,
        daemon_id: str,
        process_identity: ProcessIdentity,
        containment_nonce: str,
        endpoint_kind: str,
        endpoint_path: str,
        started_at_utc: str,
        last_checkpoint_at_utc: str,
        status: str,
        app_root: str,
        workspace_root: str,
        version_text: str | None,
        expected_predecessor_daemon_id: str | None,
        expected_predecessor_identity: ProcessIdentity | None,
        expected_predecessor_containment_nonce: str | None,
    ) -> bool:
        """Install a launch record only against the observed ownership generation.

        The transaction accepts a daemon-published row for the new generation
        without overwriting it. Otherwise it inserts into an empty slot or
        replaces exactly the predecessor drained by the launcher. Any different
        generation is a concurrent-launch conflict.
        """
        predecessor_values = (
            expected_predecessor_daemon_id,
            expected_predecessor_identity,
            expected_predecessor_containment_nonce,
        )
        if any(value is None for value in predecessor_values) and any(
            value is not None for value in predecessor_values
        ):
            raise ValueError(
                "A provisional daemon predecessor must be complete or absent."
            )
        connection = self._store._connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            current = self.get(workspace_id)
            binding_matches = _daemon_state_binding_matches(
                current,
                workspace_id=workspace_id,
                endpoint_kind=endpoint_kind,
                endpoint_path=endpoint_path,
                app_root=app_root,
                workspace_root=workspace_root,
            )
            if binding_matches and (
                _daemon_state_generation_matches(
                    current,
                    process_identity=process_identity,
                    containment_nonce=containment_nonce,
                )
                or (
                    current is not None
                    and current.containment_nonce == containment_nonce
                    and current.daemon_id != daemon_id
                )
            ):
                connection.commit()
                return True
            predecessor_matches = (
                current is not None
                and binding_matches
                and current.daemon_id == expected_predecessor_daemon_id
                and _daemon_state_generation_matches(
                    current,
                    process_identity=expected_predecessor_identity,
                    containment_nonce=expected_predecessor_containment_nonce,
                )
            )
            if not (
                (current is None and expected_predecessor_identity is None)
                or predecessor_matches
            ):
                connection.rollback()
                return False
            self.upsert(
                workspace_id=workspace_id,
                daemon_id=daemon_id,
                pid=process_identity.pid,
                process_start_key=process_identity.start_key,
                process_executable_path=process_identity.executable_path,
                process_group_id=process_identity.process_group_id,
                process_session_id=process_identity.process_session_id,
                containment_nonce=containment_nonce,
                endpoint_kind=endpoint_kind,
                endpoint_path=endpoint_path,
                started_at_utc=started_at_utc,
                last_checkpoint_at_utc=last_checkpoint_at_utc,
                status=status,
                app_root=app_root,
                workspace_root=workspace_root,
                version_text=version_text,
            )
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()
            return True

    def get(self, workspace_id: str) -> PersistedDaemonState | None:
        row = self._store._connection().execute(
            """
            SELECT workspace_id, daemon_id, pid, process_start_key, process_executable_path,
                   process_group_id, process_session_id, containment_nonce, endpoint_kind,
                   endpoint_path, started_at_utc, last_checkpoint_at_utc, status,
                   app_root, workspace_root, version_text
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
            daemon_id=(str(row["daemon_id"]) if row["daemon_id"] is not None else None),
            process_start_key=(
                str(row["process_start_key"])
                if row["process_start_key"] is not None
                else None
            ),
            process_executable_path=(
                str(row["process_executable_path"])
                if row["process_executable_path"] is not None
                else None
            ),
            process_group_id=(
                int(row["process_group_id"])
                if row["process_group_id"] is not None
                else None
            ),
            process_session_id=(
                int(row["process_session_id"])
                if row["process_session_id"] is not None
                else None
            ),
            containment_nonce=(
                str(row["containment_nonce"])
                if row["containment_nonce"] is not None
                else None
            ),
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

    def clear_workspace(self, workspace_id: str) -> None:
        """Delete all client-session rows for one workspace."""
        self._store._connection().execute(
            "DELETE FROM client_sessions WHERE workspace_id = ?",
            (workspace_id,),
        )


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
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS daemon_state (
                    workspace_id TEXT PRIMARY KEY,
                    daemon_id TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    process_start_key TEXT NOT NULL,
                    process_executable_path TEXT NOT NULL,
                    process_group_id INTEGER,
                    process_session_id INTEGER,
                    containment_nonce TEXT NOT NULL,
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
            daemon_state_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(daemon_state)").fetchall()
            }
            for column_name, migration in (
                ("daemon_id", "ALTER TABLE daemon_state ADD COLUMN daemon_id TEXT"),
                (
                    "process_start_key",
                    "ALTER TABLE daemon_state ADD COLUMN process_start_key TEXT",
                ),
                (
                    "process_executable_path",
                    "ALTER TABLE daemon_state ADD COLUMN process_executable_path TEXT",
                ),
                (
                    "process_group_id",
                    "ALTER TABLE daemon_state ADD COLUMN process_group_id INTEGER",
                ),
                (
                    "process_session_id",
                    "ALTER TABLE daemon_state ADD COLUMN process_session_id INTEGER",
                ),
                (
                    "containment_nonce",
                    "ALTER TABLE daemon_state ADD COLUMN containment_nonce TEXT",
                ),
            ):
                if column_name not in daemon_state_columns:
                    connection.execute(migration)
            connection.execute(
                """
                DELETE FROM daemon_state
                WHERE daemon_id IS NULL
                   OR process_start_key IS NULL
                   OR process_executable_path IS NULL
                   OR containment_nonce IS NULL
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
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_client_sessions_workspace
                ON client_sessions(workspace_id, updated_at_utc DESC)
                """
            )
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()
        self._checkpoint_wal(passive=True)

    def reset_workspace(self, workspace_id: str) -> None:
        """Delete client sessions while retaining the daemon identity tombstone."""
        connection = self._connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            self.client_sessions.clear_workspace(workspace_id)
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()


__all__ = ["ClientSessionRepository", "DaemonStateRepository", "RuntimeControlLedger"]
