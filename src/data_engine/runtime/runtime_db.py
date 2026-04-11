"""SQLite-backed runtime ledger for flow lifecycle, staleness, and log history."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import sqlite3
import threading
from typing import Self

from data_engine.domain.source_state import SourceSignature
from data_engine.domain.time import parse_utc_text, utcnow_text
from data_engine.platform.paths import normalized_path_text, stable_absolute_path
from data_engine.platform.workspace_models import (
    DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR,
    DATA_ENGINE_RUNTIME_CONTROL_DB_PATH_ENV_VAR,
    DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR,
)
from data_engine.platform.processes import process_is_running
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.ledger_models import (
    PersistedDaemonState,
    PersistedFileState,
    PersistedLogEntry,
    PersistedRun,
    PersistedStepRun,
)

class _RuntimeSqliteStore:
    """Own one SQLite-backed runtime store and expose narrow read/write helpers."""

    HISTORY_RETENTION_DAYS = 30

    def __init__(self, db_path: Path) -> None:
        self.db_path = stable_absolute_path(db_path)
        self._connections: dict[int, sqlite3.Connection] = {}
        self._connections_lock = threading.RLock()
        self._ensure_parent_dir()
        self._initialize_schema()

    def _ensure_parent_dir(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connection(self) -> sqlite3.Connection:
        thread_id = threading.get_ident()
        with self._connections_lock:
            connection = self._connections.get(thread_id)
            if connection is None:
                connection = sqlite3.connect(
                    self.db_path,
                    timeout=5.0,
                    isolation_level=None,
                    check_same_thread=False,
                )
                connection.row_factory = sqlite3.Row
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA busy_timeout = 5000")
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA wal_autocheckpoint = 100")
                self._connections[thread_id] = connection
            return connection

    def close(self) -> None:
        """Close all SQLite connections opened for this ledger across threads."""
        with self._connections_lock:
            connections = tuple(self._connections.values())
            self._connections.clear()
        for connection in connections:
            connection.close()

    def __del__(self) -> None:
        """Best-effort cleanup for ledger connections when callers forget to close."""
        try:
            self.close()
        except Exception:
            pass

    def _initialize_schema(self) -> None:
        raise NotImplementedError

    def _checkpoint_wal(self, *, passive: bool = False) -> None:
        """Best-effort WAL checkpointing to avoid indefinite growth on long-lived sessions."""
        mode = "PASSIVE" if passive else "TRUNCATE"
        try:
            self._connection().execute(f"PRAGMA wal_checkpoint({mode})")
        except sqlite3.Error:
            pass


class _RuntimeCacheSchema(_RuntimeSqliteStore):
    """Own the cache/runtime-history SQLite store for runs, logs, and file state."""

    @classmethod
    def open_default(cls, *, data_root: Path | None = None) -> Self:
        """Open the default workspace runtime cache ledger."""
        env_override_raw = os.environ.get(DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR)
        if env_override_raw is None or not env_override_raw.strip():
            env_override_raw = os.environ.get(DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR)
        if env_override_raw is not None and env_override_raw.strip():
            return cls(Path(env_override_raw).expanduser().resolve())
        return cls(RuntimeLayoutPolicy().resolve_paths(data_root=data_root).runtime_cache_db_path)

    def _initialize_schema(self) -> None:
        connection = self._connection()
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                flow_name TEXT NOT NULL,
                group_name TEXT NOT NULL,
                source_path TEXT,
                status TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                finished_at_utc TEXT,
                error_text TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS step_runs (
                id INTEGER PRIMARY KEY,
                run_id TEXT NOT NULL,
                flow_name TEXT NOT NULL,
                step_label TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                finished_at_utc TEXT,
                elapsed_ms INTEGER,
                error_text TEXT,
                output_path TEXT,
                FOREIGN KEY (run_id) REFERENCES runs(run_id)
            )
            """
        )
        columns = {str(row["name"]) for row in connection.execute("PRAGMA table_info(step_runs)").fetchall()}
        if "output_path" not in columns:
            connection.execute("ALTER TABLE step_runs ADD COLUMN output_path TEXT")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS file_state (
                flow_name TEXT NOT NULL,
                source_path TEXT NOT NULL,
                mtime_ns INTEGER NOT NULL,
                size_bytes INTEGER NOT NULL,
                last_success_run_id TEXT,
                last_success_at_utc TEXT,
                last_status TEXT NOT NULL,
                last_error_text TEXT,
                PRIMARY KEY (flow_name, source_path)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY,
                run_id TEXT,
                flow_name TEXT,
                step_label TEXT,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at_utc TEXT NOT NULL
            )
            """
        )
        connection.execute("CREATE INDEX IF NOT EXISTS idx_runs_flow_started ON runs(flow_name, started_at_utc DESC)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_step_runs_run ON step_runs(run_id, id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_logs_flow_created ON logs(flow_name, created_at_utc, id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_logs_run_created ON logs(run_id, created_at_utc, id)")
        self._checkpoint_wal(passive=True)


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
        """Insert or replace one daemon metadata row."""
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
        """Return daemon metadata for one workspace when present."""
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
        """Delete daemon metadata for one workspace."""
        self._store._connection().execute("DELETE FROM daemon_state WHERE workspace_id = ?", (workspace_id,))


class ClientSessionRepository:
    """Repository for persisted local UI/client sessions."""

    def __init__(self, store: _RuntimeSqliteStore) -> None:
        self._store = store

    def upsert(
        self,
        *,
        client_id: str,
        workspace_id: str,
        client_kind: str,
        pid: int,
    ) -> None:
        """Insert or refresh one local client session row."""
        row = self._store._connection().execute(
            "SELECT started_at_utc FROM client_sessions WHERE client_id = ?",
            (client_id,),
        ).fetchone()
        started_at_utc = str(row["started_at_utc"]) if row is not None and row["started_at_utc"] else utcnow_text()
        updated_at_utc = utcnow_text()
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
        """Delete one local client session row."""
        self._store._connection().execute("DELETE FROM client_sessions WHERE client_id = ?", (client_id,))

    def remove_for_process(self, *, workspace_id: str, client_kind: str, pid: int) -> None:
        """Delete all client session rows for one workspace/client-kind/process tuple."""
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
        """Return the number of live client sessions for one workspace."""
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
    def open_default(cls, *, data_root: Path | None = None) -> "RuntimeControlLedger":
        """Open the default workspace runtime control ledger."""
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

class _RuntimeCacheOperations:
    """Runtime history operations mixed into the cache ledger only."""

    def normalize_source_path(self, source_path: Path | str) -> str:
        """Normalize a source path for stable persistence and comparisons."""
        return normalized_path_text(stable_absolute_path(source_path))

    def source_signature_for_path(self, source_path: Path) -> SourceSignature | None:
        """Return the current source signature when the file exists."""
        try:
            stat = source_path.stat()
        except FileNotFoundError:
            return None
        return SourceSignature(
            source_path=_RuntimeCacheOperations.normalize_source_path(self, source_path),
            mtime_ns=stat.st_mtime_ns,
            size_bytes=stat.st_size,
        )

    def is_poll_source_stale(self, flow_name: str, signature: SourceSignature | None) -> bool:
        """Return whether a concrete source signature should be rerun."""
        if signature is None:
            return False
        row = self._connection().execute(
            """
            SELECT mtime_ns, size_bytes, last_status
            FROM file_state
            WHERE flow_name = ? AND source_path = ?
            """,
            (flow_name, signature.source_path),
        ).fetchone()
        if row is None:
            return True
        if int(row["mtime_ns"]) != signature.mtime_ns or int(row["size_bytes"]) != signature.size_bytes:
            return True
        return str(row["last_status"]) != "success"

    def record_run_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        group_name: str,
        source_path: str | None,
        started_at_utc: str,
    ) -> None:
        """Insert one started run row."""
        self._connection().execute(
            """
            INSERT INTO runs(run_id, flow_name, group_name, source_path, status, started_at_utc)
            VALUES (?, ?, ?, ?, 'started', ?)
            """,
            (run_id, flow_name, group_name, source_path, started_at_utc),
        )

    def record_run_finished(
        self,
        *,
        run_id: str,
        status: str,
        finished_at_utc: str,
        error_text: str | None = None,
    ) -> None:
        """Finalize one persisted run row."""
        self._connection().execute(
            """
            UPDATE runs
            SET status = ?, finished_at_utc = ?, error_text = ?
            WHERE run_id = ?
            """,
            (status, finished_at_utc, error_text, run_id),
        )
        _RuntimeCacheOperations.prune_history(self, retention_days=self.HISTORY_RETENTION_DAYS)

    def record_step_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        step_label: str,
        started_at_utc: str,
    ) -> int:
        """Insert one started step row and return its surrogate key."""
        cursor = self._connection().execute(
            """
            INSERT INTO step_runs(run_id, flow_name, step_label, status, started_at_utc)
            VALUES (?, ?, ?, 'started', ?)
            """,
            (run_id, flow_name, step_label, started_at_utc),
        )
        return int(cursor.lastrowid)

    def record_step_finished(
        self,
        *,
        step_run_id: int,
        status: str,
        finished_at_utc: str,
        elapsed_ms: int | None,
        error_text: str | None = None,
        output_path: str | None = None,
    ) -> None:
        """Finalize one persisted step row."""
        self._connection().execute(
            """
            UPDATE step_runs
            SET status = ?, finished_at_utc = ?, elapsed_ms = ?, error_text = ?, output_path = ?
            WHERE id = ?
            """,
            (status, finished_at_utc, elapsed_ms, error_text, output_path, step_run_id),
        )

    def upsert_file_state(
        self,
        *,
        flow_name: str,
        signature: SourceSignature,
        status: str,
        run_id: str | None = None,
        finished_at_utc: str | None = None,
        error_text: str | None = None,
    ) -> None:
        """Upsert one file-state row for a polled source file."""
        success_run_id = run_id if status == "success" else None
        success_at = finished_at_utc if status == "success" else None
        self._connection().execute(
            """
            INSERT INTO file_state(
                flow_name,
                source_path,
                mtime_ns,
                size_bytes,
                last_success_run_id,
                last_success_at_utc,
                last_status,
                last_error_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(flow_name, source_path) DO UPDATE SET
                mtime_ns = excluded.mtime_ns,
                size_bytes = excluded.size_bytes,
                last_success_run_id = CASE
                    WHEN excluded.last_status = 'success' THEN excluded.last_success_run_id
                    ELSE file_state.last_success_run_id
                END,
                last_success_at_utc = CASE
                    WHEN excluded.last_status = 'success' THEN excluded.last_success_at_utc
                    ELSE file_state.last_success_at_utc
                END,
                last_status = excluded.last_status,
                last_error_text = excluded.last_error_text
            """,
            (
                flow_name,
                signature.source_path,
                signature.mtime_ns,
                signature.size_bytes,
                success_run_id,
                success_at,
                status,
                error_text,
            ),
        )

    def append_log(
        self,
        *,
        level: str,
        message: str,
        created_at_utc: str,
        run_id: str | None = None,
        flow_name: str | None = None,
        step_label: str | None = None,
    ) -> None:
        """Persist one runtime log line."""
        self._connection().execute(
            """
            INSERT INTO logs(run_id, flow_name, step_label, level, message, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, flow_name, step_label, level, message, created_at_utc),
        )

    def prune_history(self, *, retention_days: int) -> None:
        """Delete run, step, and log history older than the retention window."""
        if retention_days <= 0:
            raise ValueError("retention_days must be positive.")
        cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
        connection = self._connection()
        stale_run_ids = tuple(
            str(row["run_id"])
            for row in connection.execute(
                """
                SELECT run_id
                FROM runs
                WHERE COALESCE(finished_at_utc, started_at_utc) < ?
                """,
                (cutoff,),
            ).fetchall()
        )
        if not stale_run_ids:
            return
        placeholders = ", ".join("?" for _ in stale_run_ids)
        connection.execute(f"DELETE FROM logs WHERE run_id IN ({placeholders})", stale_run_ids)
        connection.execute(f"DELETE FROM step_runs WHERE run_id IN ({placeholders})", stale_run_ids)
        connection.execute(f"DELETE FROM runs WHERE run_id IN ({placeholders})", stale_run_ids)
        connection.execute(
            f"""
            UPDATE file_state
            SET last_success_run_id = NULL
            WHERE last_success_run_id IN ({placeholders})
            """,
            stale_run_ids,
        )

    def prune_missing_file_state(self, *, flow_name: str, current_source_paths: set[str]) -> None:
        """Delete file-state rows for one flow when the source file no longer exists."""
        connection = self._connection()
        rows = connection.execute(
            """
            SELECT source_path
            FROM file_state
            WHERE flow_name = ?
            """,
            (flow_name,),
        ).fetchall()
        stale_paths = [
            str(row["source_path"])
            for row in rows
            if str(row["source_path"]) not in current_source_paths
        ]
        if not stale_paths:
            return
        placeholders = ", ".join("?" for _ in stale_paths)
        connection.execute(
            f"DELETE FROM file_state WHERE flow_name = ? AND source_path IN ({placeholders})",
            (flow_name, *stale_paths),
        )

    def list_logs(self, *, flow_name: str | None = None, run_id: str | None = None) -> tuple[PersistedLogEntry, ...]:
        """Return persisted runtime logs in creation order."""
        clauses: list[str] = []
        params: list[object] = []
        if flow_name is not None:
            clauses.append("flow_name = ?")
            params.append(flow_name)
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._connection().execute(
            f"""
            SELECT id, run_id, flow_name, step_label, level, message, created_at_utc
            FROM logs
            {where}
            ORDER BY created_at_utc, id
            """,
            params,
        ).fetchall()
        return tuple(
            PersistedLogEntry(
                id=int(row["id"]),
                run_id=row["run_id"],
                flow_name=row["flow_name"],
                step_label=row["step_label"],
                level=str(row["level"]),
                message=str(row["message"]),
                created_at_utc=str(row["created_at_utc"]),
            )
            for row in rows
        )

    def list_runs(self, *, flow_name: str | None = None) -> tuple[PersistedRun, ...]:
        """Return persisted runs, newest first."""
        if flow_name is None:
            rows = self._connection().execute(
                """
                SELECT run_id, flow_name, group_name, source_path, status, started_at_utc, finished_at_utc, error_text
                FROM runs
                ORDER BY started_at_utc DESC, run_id DESC
                """
            ).fetchall()
        else:
            rows = self._connection().execute(
                """
                SELECT run_id, flow_name, group_name, source_path, status, started_at_utc, finished_at_utc, error_text
                FROM runs
                WHERE flow_name = ?
                ORDER BY started_at_utc DESC, run_id DESC
                """,
                (flow_name,),
            ).fetchall()
        return tuple(
            PersistedRun(
                run_id=str(row["run_id"]),
                flow_name=str(row["flow_name"]),
                group_name=str(row["group_name"]),
                source_path=row["source_path"],
                status=str(row["status"]),
                started_at_utc=str(row["started_at_utc"]),
                finished_at_utc=row["finished_at_utc"],
                error_text=row["error_text"],
            )
            for row in rows
        )

    def list_step_runs(self, run_id: str) -> tuple[PersistedStepRun, ...]:
        """Return persisted step runs for one run id."""
        rows = self._connection().execute(
            """
            SELECT id, run_id, flow_name, step_label, status, started_at_utc, finished_at_utc, elapsed_ms, error_text, output_path
            FROM step_runs
            WHERE run_id = ?
            ORDER BY id
            """,
            (run_id,),
        ).fetchall()
        return tuple(
            PersistedStepRun(
                id=int(row["id"]),
                run_id=str(row["run_id"]),
                flow_name=str(row["flow_name"]),
                step_label=str(row["step_label"]),
                status=str(row["status"]),
                started_at_utc=str(row["started_at_utc"]),
                finished_at_utc=row["finished_at_utc"],
                elapsed_ms=row["elapsed_ms"],
                error_text=row["error_text"],
                output_path=row["output_path"],
            )
            for row in rows
        )

    def list_file_states(self, *, flow_name: str | None = None) -> tuple[PersistedFileState, ...]:
        """Return current persisted file-state rows."""
        if flow_name is None:
            rows = self._connection().execute(
                """
                SELECT flow_name, source_path, mtime_ns, size_bytes, last_success_run_id, last_success_at_utc, last_status, last_error_text
                FROM file_state
                ORDER BY flow_name, source_path
                """
            ).fetchall()
        else:
            rows = self._connection().execute(
                """
                SELECT flow_name, source_path, mtime_ns, size_bytes, last_success_run_id, last_success_at_utc, last_status, last_error_text
                FROM file_state
                WHERE flow_name = ?
                ORDER BY source_path
                """,
                (flow_name,),
            ).fetchall()
        return tuple(
            PersistedFileState(
                flow_name=str(row["flow_name"]),
                source_path=str(row["source_path"]),
                mtime_ns=int(row["mtime_ns"]),
                size_bytes=int(row["size_bytes"]),
                last_success_run_id=row["last_success_run_id"],
                last_success_at_utc=row["last_success_at_utc"],
                last_status=str(row["last_status"]),
                last_error_text=row["last_error_text"],
            )
            for row in rows
        )

    def replace_runs(self, rows: tuple[PersistedRun, ...]) -> None:
        """Replace all persisted run rows with one snapshot."""
        connection = self._connection()
        connection.execute("DELETE FROM step_runs")
        connection.execute("DELETE FROM logs")
        connection.execute("DELETE FROM runs")
        if not rows:
            return
        connection.executemany(
            """
            INSERT INTO runs(run_id, flow_name, group_name, source_path, status, started_at_utc, finished_at_utc, error_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.run_id,
                    row.flow_name,
                    row.group_name,
                    row.source_path,
                    row.status,
                    row.started_at_utc,
                    row.finished_at_utc,
                    row.error_text,
                )
                for row in rows
            ],
        )

    def replace_step_runs(self, rows: tuple[PersistedStepRun, ...]) -> None:
        """Replace all persisted step rows with one snapshot."""
        connection = self._connection()
        connection.execute("DELETE FROM step_runs")
        if not rows:
            return
        connection.executemany(
            """
            INSERT INTO step_runs(id, run_id, flow_name, step_label, status, started_at_utc, finished_at_utc, elapsed_ms, error_text, output_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.id,
                    row.run_id,
                    row.flow_name,
                    row.step_label,
                    row.status,
                    row.started_at_utc,
                    row.finished_at_utc,
                    row.elapsed_ms,
                    row.error_text,
                    row.output_path,
                )
                for row in rows
            ],
        )

    def replace_logs(self, rows: tuple[PersistedLogEntry, ...]) -> None:
        """Replace all persisted log rows with one snapshot."""
        connection = self._connection()
        connection.execute("DELETE FROM logs")
        if not rows:
            return
        connection.executemany(
            """
            INSERT INTO logs(id, run_id, flow_name, step_label, level, message, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.id,
                    row.run_id,
                    row.flow_name,
                    row.step_label,
                    row.level,
                    row.message,
                    row.created_at_utc,
                )
                for row in rows
            ],
        )

    def replace_file_states(self, rows: tuple[PersistedFileState, ...]) -> None:
        """Replace all persisted file-state rows with one snapshot."""
        connection = self._connection()
        connection.execute("DELETE FROM file_state")
        if not rows:
            return
        connection.executemany(
            """
            INSERT INTO file_state(flow_name, source_path, mtime_ns, size_bytes, last_success_run_id, last_success_at_utc, last_status, last_error_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.flow_name,
                    row.source_path,
                    row.mtime_ns,
                    row.size_bytes,
                    row.last_success_run_id,
                    row.last_success_at_utc,
                    row.last_status,
                    row.last_error_text,
                )
                for row in rows
            ],
        )

    def replace_runtime_snapshot(
        self,
        *,
        runs: tuple[PersistedRun, ...],
        step_runs: tuple[PersistedStepRun, ...],
        logs: tuple[PersistedLogEntry, ...],
        file_states: tuple[PersistedFileState, ...],
    ) -> None:
        """Replace the runtime snapshot tables in foreign-key-safe order."""
        connection = self._connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute("DELETE FROM step_runs")
            connection.execute("DELETE FROM logs")
            connection.execute("DELETE FROM runs")
            connection.execute("DELETE FROM file_state")
            if runs:
                connection.executemany(
                    """
                    INSERT INTO runs(run_id, flow_name, group_name, source_path, status, started_at_utc, finished_at_utc, error_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row.run_id,
                            row.flow_name,
                            row.group_name,
                            row.source_path,
                            row.status,
                            row.started_at_utc,
                            row.finished_at_utc,
                            row.error_text,
                        )
                        for row in runs
                    ],
                )
            if step_runs:
                connection.executemany(
                    """
                    INSERT INTO step_runs(run_id, flow_name, step_label, status, started_at_utc, finished_at_utc, elapsed_ms, error_text, output_path)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row.run_id,
                            row.flow_name,
                            row.step_label,
                            row.status,
                            row.started_at_utc,
                            row.finished_at_utc,
                            row.elapsed_ms,
                            row.error_text,
                            row.output_path,
                        )
                        for row in step_runs
                    ],
                )
            if logs:
                connection.executemany(
                    """
                    INSERT INTO logs(run_id, flow_name, step_label, level, message, created_at_utc)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row.run_id,
                            row.flow_name,
                            row.step_label,
                            row.level,
                            row.message,
                            row.created_at_utc,
                        )
                        for row in logs
                    ],
                )
            if file_states:
                deduped_file_states: dict[tuple[str, str], PersistedFileState] = {}
                for row in file_states:
                    deduped_file_states[(row.flow_name, row.source_path)] = row
                connection.executemany(
                    """
                    INSERT INTO file_state(flow_name, source_path, mtime_ns, size_bytes, last_success_run_id, last_success_at_utc, last_status, last_error_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            row.flow_name,
                            row.source_path,
                            row.mtime_ns,
                            row.size_bytes,
                            row.last_success_run_id,
                            row.last_success_at_utc,
                            row.last_status,
                            row.last_error_text,
                        )
                        for row in deduped_file_states.values()
                    ],
                )
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()


class RuntimeRunRepository:
    """Repository for persisted flow run lifecycle rows."""

    def __init__(self, store: _RuntimeCacheSchema) -> None:
        self._store = store

    def record_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        group_name: str,
        source_path: str | None,
        started_at_utc: str,
    ) -> None:
        """Insert one started run row."""
        _RuntimeCacheOperations.record_run_started(
            self._store,
            run_id=run_id,
            flow_name=flow_name,
            group_name=group_name,
            source_path=source_path,
            started_at_utc=started_at_utc,
        )

    def record_finished(
        self,
        *,
        run_id: str,
        status: str,
        finished_at_utc: str,
        error_text: str | None = None,
    ) -> None:
        """Finalize one persisted run row."""
        _RuntimeCacheOperations.record_run_finished(
            self._store,
            run_id=run_id,
            status=status,
            finished_at_utc=finished_at_utc,
            error_text=error_text,
        )

    def list(self, *, flow_name: str | None = None) -> tuple[PersistedRun, ...]:
        """Return persisted runs, newest first."""
        return _RuntimeCacheOperations.list_runs(self._store, flow_name=flow_name)

    def replace(self, rows: tuple[PersistedRun, ...]) -> None:
        """Replace all persisted run rows with one snapshot."""
        _RuntimeCacheOperations.replace_runs(self._store, rows)

    def prune_history(self, *, retention_days: int) -> None:
        """Delete run, step, and log history older than the retention window."""
        _RuntimeCacheOperations.prune_history(self._store, retention_days=retention_days)


class RuntimeStepOutputRepository:
    """Repository for persisted step execution and output rows."""

    def __init__(self, store: _RuntimeCacheSchema) -> None:
        self._store = store

    def record_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        step_label: str,
        started_at_utc: str,
    ) -> int:
        """Insert one started step row and return its surrogate key."""
        return _RuntimeCacheOperations.record_step_started(
            self._store,
            run_id=run_id,
            flow_name=flow_name,
            step_label=step_label,
            started_at_utc=started_at_utc,
        )

    def record_finished(
        self,
        *,
        step_run_id: int,
        status: str,
        finished_at_utc: str,
        elapsed_ms: int | None,
        error_text: str | None = None,
        output_path: str | None = None,
    ) -> None:
        """Finalize one persisted step row."""
        _RuntimeCacheOperations.record_step_finished(
            self._store,
            step_run_id=step_run_id,
            status=status,
            finished_at_utc=finished_at_utc,
            elapsed_ms=elapsed_ms,
            error_text=error_text,
            output_path=output_path,
        )

    def list_for_run(self, run_id: str) -> tuple[PersistedStepRun, ...]:
        """Return persisted step runs for one run id."""
        return _RuntimeCacheOperations.list_step_runs(self._store, run_id)

    def replace(self, rows: tuple[PersistedStepRun, ...]) -> None:
        """Replace all persisted step rows with one snapshot."""
        _RuntimeCacheOperations.replace_step_runs(self._store, rows)


class SourceSignatureRepository:
    """Repository for source signatures and poll freshness rows."""

    def __init__(self, store: _RuntimeCacheSchema) -> None:
        self._store = store

    def normalize_path(self, source_path: Path | str) -> str:
        """Normalize a source path for stable persistence and comparisons."""
        return _RuntimeCacheOperations.normalize_source_path(self._store, source_path)

    def signature_for_path(self, source_path: Path) -> SourceSignature | None:
        """Return the current source signature when the file exists."""
        return _RuntimeCacheOperations.source_signature_for_path(self._store, source_path)

    def is_stale(self, flow_name: str, signature: SourceSignature | None) -> bool:
        """Return whether a concrete source signature should be rerun."""
        return _RuntimeCacheOperations.is_poll_source_stale(self._store, flow_name, signature)

    def upsert_file_state(
        self,
        *,
        flow_name: str,
        signature: SourceSignature,
        status: str,
        run_id: str | None = None,
        finished_at_utc: str | None = None,
        error_text: str | None = None,
    ) -> None:
        """Upsert one file-state row for a polled source file."""
        _RuntimeCacheOperations.upsert_file_state(
            self._store,
            flow_name=flow_name,
            signature=signature,
            status=status,
            run_id=run_id,
            finished_at_utc=finished_at_utc,
            error_text=error_text,
        )

    def prune_missing(self, *, flow_name: str, current_source_paths: set[str]) -> None:
        """Delete file-state rows for one flow when the source file no longer exists."""
        _RuntimeCacheOperations.prune_missing_file_state(
            self._store,
            flow_name=flow_name,
            current_source_paths=current_source_paths,
        )

    def list_file_states(self, *, flow_name: str | None = None) -> tuple[PersistedFileState, ...]:
        """Return current persisted file-state rows."""
        return _RuntimeCacheOperations.list_file_states(self._store, flow_name=flow_name)

    def replace_file_states(self, rows: tuple[PersistedFileState, ...]) -> None:
        """Replace all persisted file-state rows with one snapshot."""
        _RuntimeCacheOperations.replace_file_states(self._store, rows)


class RuntimeLogRepository:
    """Repository for persisted runtime log rows."""

    def __init__(self, store: _RuntimeCacheSchema) -> None:
        self._store = store

    def append(
        self,
        *,
        level: str,
        message: str,
        created_at_utc: str,
        run_id: str | None = None,
        flow_name: str | None = None,
        step_label: str | None = None,
    ) -> None:
        """Persist one runtime log line."""
        _RuntimeCacheOperations.append_log(
            self._store,
            level=level,
            message=message,
            created_at_utc=created_at_utc,
            run_id=run_id,
            flow_name=flow_name,
            step_label=step_label,
        )

    def list(self, *, flow_name: str | None = None, run_id: str | None = None) -> tuple[PersistedLogEntry, ...]:
        """Return persisted runtime logs in creation order."""
        return _RuntimeCacheOperations.list_logs(self._store, flow_name=flow_name, run_id=run_id)

    def replace(self, rows: tuple[PersistedLogEntry, ...]) -> None:
        """Replace all persisted log rows with one snapshot."""
        _RuntimeCacheOperations.replace_logs(self._store, rows)


class RuntimeSnapshotRepository:
    """Repository for atomic runtime snapshot replacement."""

    def __init__(self, store: _RuntimeCacheSchema) -> None:
        self._store = store

    def replace(
        self,
        *,
        runs: tuple[PersistedRun, ...],
        step_runs: tuple[PersistedStepRun, ...],
        logs: tuple[PersistedLogEntry, ...],
        file_states: tuple[PersistedFileState, ...],
    ) -> None:
        """Replace the runtime snapshot tables in foreign-key-safe order."""
        _RuntimeCacheOperations.replace_runtime_snapshot(
            self._store,
            runs=runs,
            step_runs=step_runs,
            logs=logs,
            file_states=file_states,
        )


class RuntimeExecutionStateRepository:
    """Repository facade for the state writes needed during one flow execution."""

    def __init__(
        self,
        *,
        runs: RuntimeRunRepository,
        step_outputs: RuntimeStepOutputRepository,
        source_signatures: SourceSignatureRepository,
    ) -> None:
        self.runs = runs
        self.step_outputs = step_outputs
        self.source_signatures = source_signatures

    def record_run_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        group_name: str,
        source_path: str | None,
        started_at_utc: str,
    ) -> None:
        """Record that one flow run started."""
        self.runs.record_started(
            run_id=run_id,
            flow_name=flow_name,
            group_name=group_name,
            source_path=source_path,
            started_at_utc=started_at_utc,
        )

    def record_run_finished(
        self,
        *,
        run_id: str,
        status: str,
        finished_at_utc: str,
        error_text: str | None = None,
    ) -> None:
        """Record that one flow run finished."""
        self.runs.record_finished(run_id=run_id, status=status, finished_at_utc=finished_at_utc, error_text=error_text)

    def record_step_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        step_label: str,
        started_at_utc: str,
    ) -> int:
        """Record that one step started and return the persisted step id."""
        return self.step_outputs.record_started(run_id=run_id, flow_name=flow_name, step_label=step_label, started_at_utc=started_at_utc)

    def record_step_finished(
        self,
        *,
        step_run_id: int,
        status: str,
        finished_at_utc: str,
        elapsed_ms: int | None,
        error_text: str | None = None,
        output_path: str | None = None,
    ) -> None:
        """Record that one step finished."""
        self.step_outputs.record_finished(
            step_run_id=step_run_id,
            status=status,
            finished_at_utc=finished_at_utc,
            elapsed_ms=elapsed_ms,
            error_text=error_text,
            output_path=output_path,
        )

    def upsert_file_state(
        self,
        *,
        flow_name: str,
        signature: SourceSignature,
        status: str,
        run_id: str | None = None,
        finished_at_utc: str | None = None,
        error_text: str | None = None,
    ) -> None:
        """Write source freshness state for one polled file."""
        self.source_signatures.upsert_file_state(
            flow_name=flow_name,
            signature=signature,
            status=status,
            run_id=run_id,
            finished_at_utc=finished_at_utc,
            error_text=error_text,
        )


class RuntimeCacheLedger(_RuntimeCacheSchema):
    """Own the cache/runtime-history SQLite store for runs, logs, and file state."""

    def __init__(self, db_path: Path) -> None:
        super().__init__(db_path)
        self.runs = RuntimeRunRepository(self)
        self.step_outputs = RuntimeStepOutputRepository(self)
        self.source_signatures = SourceSignatureRepository(self)
        self.logs = RuntimeLogRepository(self)
        self.snapshots = RuntimeSnapshotRepository(self)
        self.execution_state = RuntimeExecutionStateRepository(
            runs=self.runs,
            step_outputs=self.step_outputs,
            source_signatures=self.source_signatures,
        )

__all__ = [
    "ClientSessionRepository",
    "DaemonStateRepository",
    "RuntimeCacheLedger",
    "RuntimeControlLedger",
    "RuntimeExecutionStateRepository",
    "RuntimeLogRepository",
    "RuntimeRunRepository",
    "RuntimeSnapshotRepository",
    "RuntimeStepOutputRepository",
    "SourceSignatureRepository",
    "parse_utc_text",
    "utcnow_text",
]
