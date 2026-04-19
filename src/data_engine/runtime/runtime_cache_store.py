"""SQLite-backed runtime cache store for runs, step outputs, logs, and source freshness."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
from typing import Self

from data_engine.domain.time import utcnow_text
from data_engine.domain.source_state import SourceSignature
from data_engine.platform.paths import normalized_path_text, stable_absolute_path
from data_engine.platform.workspace_models import (
    DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR,
    DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR,
)
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.ledger_models import (
    PersistedFileState,
    PersistedLogEntry,
    PersistedRun,
    PersistedStepRun,
)
from data_engine.runtime.sqlite_store import _RuntimeSqliteStore


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
        started_at_utc: str | None,
    ) -> None:
        effective_started_at_utc = utcnow_text() if started_at_utc is None else started_at_utc
        self._store._connection().execute(
            """
            INSERT INTO runs(run_id, flow_name, group_name, source_path, status, started_at_utc)
            VALUES (?, ?, ?, ?, 'started', ?)
            """,
            (run_id, flow_name, group_name, source_path, effective_started_at_utc),
        )

    def record_finished(
        self,
        *,
        run_id: str,
        status: str,
        finished_at_utc: str,
        error_text: str | None = None,
    ) -> None:
        self._store._connection().execute(
            """
            UPDATE runs
            SET status = ?, finished_at_utc = ?, error_text = ?
            WHERE run_id = ?
            """,
            (status, finished_at_utc, error_text, run_id),
        )
        self.prune_history(retention_days=self._store.HISTORY_RETENTION_DAYS)

    def get(self, run_id: str) -> PersistedRun | None:
        row = self._store._connection().execute(
            """
            SELECT run_id, flow_name, group_name, source_path, status, started_at_utc, finished_at_utc, error_text
            FROM runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        return PersistedRun(
            run_id=str(row["run_id"]),
            flow_name=str(row["flow_name"]),
            group_name=str(row["group_name"]),
            source_path=row["source_path"],
            status=str(row["status"]),
            started_at_utc=str(row["started_at_utc"]),
            finished_at_utc=row["finished_at_utc"],
            error_text=row["error_text"],
        )

    def finish_active(
        self,
        *,
        status: str,
        finished_at_utc: str,
        error_text: str | None = None,
    ) -> int:
        """Mark all nonterminal runs as finished and return the affected row count."""
        cursor = self._store._connection().execute(
            """
            UPDATE runs
            SET status = ?, finished_at_utc = ?, error_text = COALESCE(error_text, ?)
            WHERE status NOT IN ('success', 'failed', 'stopped')
            """,
            (status, finished_at_utc, error_text),
        )
        return int(cursor.rowcount or 0)

    def list(self, *, flow_name: str | None = None) -> tuple[PersistedRun, ...]:
        if flow_name is None:
            rows = self._store._connection().execute(
                """
                SELECT run_id, flow_name, group_name, source_path, status, started_at_utc, finished_at_utc, error_text
                FROM runs
                ORDER BY started_at_utc DESC, run_id DESC
                """
            ).fetchall()
        else:
            rows = self._store._connection().execute(
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

    def list_active(self, *, flow_name: str | None = None) -> tuple[PersistedRun, ...]:
        """Return runs that are still in a non-terminal state."""
        if flow_name is None:
            rows = self._store._connection().execute(
                """
                SELECT run_id, flow_name, group_name, source_path, status, started_at_utc, finished_at_utc, error_text
                FROM runs
                WHERE status NOT IN ('success', 'failed', 'stopped')
                ORDER BY started_at_utc, run_id
                """
            ).fetchall()
        else:
            rows = self._store._connection().execute(
                """
                SELECT run_id, flow_name, group_name, source_path, status, started_at_utc, finished_at_utc, error_text
                FROM runs
                WHERE flow_name = ? AND status NOT IN ('success', 'failed', 'stopped')
                ORDER BY started_at_utc, run_id
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

    def replace(self, rows: tuple[PersistedRun, ...]) -> None:
        connection = self._store._connection()
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

    def delete_flow(self, flow_name: str) -> tuple[str, ...]:
        """Delete all persisted runs for one flow and return the removed run ids."""
        connection = self._store._connection()
        run_ids = tuple(
            str(row["run_id"])
            for row in connection.execute(
                """
                SELECT run_id
                FROM runs
                WHERE flow_name = ?
                """,
                (flow_name,),
            ).fetchall()
        )
        connection.execute("DELETE FROM runs WHERE flow_name = ?", (flow_name,))
        return run_ids

    def prune_history(self, *, retention_days: int) -> None:
        if retention_days <= 0:
            raise ValueError("retention_days must be positive.")
        cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
        connection = self._store._connection()
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


class RuntimeStepOutputRepository:
    """Repository for persisted step execution and output rows."""

    def __init__(self, store: _RuntimeCacheSchema) -> None:
        self._store = store

    def record_started(self, *, run_id: str, flow_name: str, step_label: str, started_at_utc: str | None) -> int:
        effective_started_at_utc = utcnow_text() if started_at_utc is None else started_at_utc
        cursor = self._store._connection().execute(
            """
            INSERT INTO step_runs(run_id, flow_name, step_label, status, started_at_utc)
            VALUES (?, ?, ?, 'started', ?)
            """,
            (run_id, flow_name, step_label, effective_started_at_utc),
        )
        return int(cursor.lastrowid)

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
        self._store._connection().execute(
            """
            UPDATE step_runs
            SET status = ?, finished_at_utc = ?, elapsed_ms = ?, error_text = ?, output_path = ?
            WHERE id = ?
            """,
            (status, finished_at_utc, elapsed_ms, error_text, output_path, step_run_id),
        )

    def get(self, step_run_id: int) -> PersistedStepRun | None:
        row = self._store._connection().execute(
            """
            SELECT id, run_id, flow_name, step_label, status, started_at_utc, finished_at_utc, elapsed_ms, error_text, output_path
            FROM step_runs
            WHERE id = ?
            """,
            (step_run_id,),
        ).fetchone()
        if row is None:
            return None
        return PersistedStepRun(
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

    def finish_active(
        self,
        *,
        status: str,
        finished_at_utc: str,
        error_text: str | None = None,
    ) -> int:
        """Mark all nonterminal step rows as finished and return the affected row count."""
        cursor = self._store._connection().execute(
            """
            UPDATE step_runs
            SET status = ?, finished_at_utc = ?, error_text = COALESCE(error_text, ?)
            WHERE status NOT IN ('success', 'failed', 'stopped')
            """,
            (status, finished_at_utc, error_text),
        )
        return int(cursor.rowcount or 0)

    def list_for_run(self, run_id: str) -> tuple[PersistedStepRun, ...]:
        rows = self._store._connection().execute(
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

    def list(
        self,
        *,
        flow_name: str | None = None,
        after_id: int | None = None,
    ) -> tuple[PersistedStepRun, ...]:
        clauses: list[str] = []
        params: list[object] = []
        if flow_name is not None:
            clauses.append("flow_name = ?")
            params.append(flow_name)
        if after_id is not None:
            clauses.append("id > ?")
            params.append(after_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._store._connection().execute(
            f"""
            SELECT id, run_id, flow_name, step_label, status, started_at_utc, finished_at_utc, elapsed_ms, error_text, output_path
            FROM step_runs
            {where}
            ORDER BY id
            """,
            params,
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

    def list_active(self, *, run_id: str | None = None) -> tuple[PersistedStepRun, ...]:
        """Return step rows that are still in a non-terminal state."""
        if run_id is None:
            rows = self._store._connection().execute(
                """
                SELECT id, run_id, flow_name, step_label, status, started_at_utc, finished_at_utc, elapsed_ms, error_text, output_path
                FROM step_runs
                WHERE status NOT IN ('success', 'failed', 'stopped')
                ORDER BY id
                """
            ).fetchall()
        else:
            rows = self._store._connection().execute(
                """
                SELECT id, run_id, flow_name, step_label, status, started_at_utc, finished_at_utc, elapsed_ms, error_text, output_path
                FROM step_runs
                WHERE run_id = ? AND status NOT IN ('success', 'failed', 'stopped')
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

    def replace(self, rows: tuple[PersistedStepRun, ...]) -> None:
        connection = self._store._connection()
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

    def delete_flow(self, flow_name: str) -> None:
        """Delete all persisted step rows for one flow."""
        self._store._connection().execute("DELETE FROM step_runs WHERE flow_name = ?", (flow_name,))


class SourceSignatureRepository:
    """Repository for source signatures and poll freshness rows."""

    def __init__(self, store: _RuntimeCacheSchema) -> None:
        self._store = store

    def normalize_path(self, source_path: Path | str) -> str:
        return normalized_path_text(stable_absolute_path(source_path))

    def signature_for_path(self, source_path: Path) -> SourceSignature | None:
        try:
            stat = source_path.stat()
        except FileNotFoundError:
            return None
        return SourceSignature(
            source_path=self.normalize_path(source_path),
            mtime_ns=stat.st_mtime_ns,
            size_bytes=stat.st_size,
        )

    def is_stale(self, flow_name: str, signature: SourceSignature | None) -> bool:
        if signature is None:
            return False
        row = self._store._connection().execute(
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
        return False

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
        success_run_id = run_id if status == "success" else None
        success_at = finished_at_utc if status == "success" else None
        self._store._connection().execute(
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

    def prune_missing(self, *, flow_name: str, current_source_paths: set[str]) -> None:
        connection = self._store._connection()
        rows = connection.execute(
            """
            SELECT source_path
            FROM file_state
            WHERE flow_name = ?
            """,
            (flow_name,),
        ).fetchall()
        stale_paths = [str(row["source_path"]) for row in rows if str(row["source_path"]) not in current_source_paths]
        if not stale_paths:
            return
        placeholders = ", ".join("?" for _ in stale_paths)
        connection.execute(
            f"DELETE FROM file_state WHERE flow_name = ? AND source_path IN ({placeholders})",
            (flow_name, *stale_paths),
        )

    def list_file_states(self, *, flow_name: str | None = None) -> tuple[PersistedFileState, ...]:
        if flow_name is None:
            rows = self._store._connection().execute(
                """
                SELECT flow_name, source_path, mtime_ns, size_bytes, last_success_run_id, last_success_at_utc, last_status, last_error_text
                FROM file_state
                ORDER BY flow_name, source_path
                """
            ).fetchall()
        else:
            rows = self._store._connection().execute(
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

    def replace_file_states(self, rows: tuple[PersistedFileState, ...]) -> None:
        connection = self._store._connection()
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

    def delete_flow(self, flow_name: str) -> None:
        """Delete all persisted source-signature rows for one flow."""
        self._store._connection().execute("DELETE FROM file_state WHERE flow_name = ?", (flow_name,))


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
        self._store._connection().execute(
            """
            INSERT INTO logs(run_id, flow_name, step_label, level, message, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, flow_name, step_label, level, message, created_at_utc),
        )

    def append_many(self, rows: tuple[PersistedLogEntry, ...]) -> None:
        """Persist multiple runtime log rows in one batch."""
        if not rows:
            return
        self._store._connection().executemany(
            """
            INSERT INTO logs(run_id, flow_name, step_label, level, message, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (row.run_id, row.flow_name, row.step_label, row.level, row.message, row.created_at_utc)
                for row in rows
            ],
        )

    def list(
        self,
        *,
        flow_name: str | None = None,
        run_id: str | None = None,
        after_id: int | None = None,
    ) -> tuple[PersistedLogEntry, ...]:
        clauses: list[str] = []
        params: list[object] = []
        if flow_name is not None:
            clauses.append("flow_name = ?")
            params.append(flow_name)
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        if after_id is not None:
            clauses.append("id > ?")
            params.append(after_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._store._connection().execute(
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

    def replace(self, rows: tuple[PersistedLogEntry, ...]) -> None:
        connection = self._store._connection()
        connection.execute("DELETE FROM logs")
        if not rows:
            return
        connection.executemany(
            """
            INSERT INTO logs(id, run_id, flow_name, step_label, level, message, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (row.id, row.run_id, row.flow_name, row.step_label, row.level, row.message, row.created_at_utc)
                for row in rows
            ],
        )

    def delete_flow(self, flow_name: str, *, run_ids: tuple[str, ...] = ()) -> None:
        """Delete all persisted log rows for one flow."""
        connection = self._store._connection()
        connection.execute("DELETE FROM logs WHERE flow_name = ?", (flow_name,))
        if not run_ids:
            return
        placeholders = ", ".join("?" for _ in run_ids)
        connection.execute(f"DELETE FROM logs WHERE run_id IN ({placeholders})", run_ids)


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
        connection = self._store._connection()
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
                    [(row.run_id, row.flow_name, row.step_label, row.level, row.message, row.created_at_utc) for row in logs],
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

    def record_run_started(self, *, run_id: str, flow_name: str, group_name: str, source_path: str | None, started_at_utc: str | None = None) -> None:
        self.runs.record_started(
            run_id=run_id,
            flow_name=flow_name,
            group_name=group_name,
            source_path=source_path,
            started_at_utc=started_at_utc,
        )

    def record_run_finished(self, *, run_id: str, status: str, finished_at_utc: str, error_text: str | None = None) -> None:
        self.runs.record_finished(run_id=run_id, status=status, finished_at_utc=finished_at_utc, error_text=error_text)

    def record_step_started(self, *, run_id: str, flow_name: str, step_label: str, started_at_utc: str | None = None) -> int:
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

    def reset_flow(self, flow_name: str) -> None:
        """Delete persisted runtime history and freshness state for one flow."""
        connection = self._connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            run_ids = tuple(run.run_id for run in self.runs.list(flow_name=flow_name))
            self.step_outputs.delete_flow(flow_name)
            self.logs.delete_flow(flow_name, run_ids=run_ids)
            self.source_signatures.delete_flow(flow_name)
            self.runs.delete_flow(flow_name)
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()

    def reset_all(self) -> None:
        """Delete all persisted runtime history and freshness state."""
        connection = self._connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute("DELETE FROM step_runs")
            connection.execute("DELETE FROM logs")
            connection.execute("DELETE FROM runs")
            connection.execute("DELETE FROM file_state")
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()

    def reconcile_orphaned_activity(
        self,
        *,
        status: str,
        finished_at_utc: str,
        error_text: str | None = None,
    ) -> tuple[int, int]:
        """Mark orphaned nonterminal runs and steps as terminal and return affected counts."""
        connection = self._connection()
        connection.execute("BEGIN IMMEDIATE")
        try:
            run_count = self.runs.finish_active(
                status=status,
                finished_at_utc=finished_at_utc,
                error_text=error_text,
            )
            step_count = self.step_outputs.finish_active(
                status=status,
                finished_at_utc=finished_at_utc,
                error_text=error_text,
            )
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()
        return run_count, step_count


__all__ = [
    "RuntimeCacheLedger",
    "RuntimeExecutionStateRepository",
    "RuntimeLogRepository",
    "RuntimeRunRepository",
    "RuntimeSnapshotRepository",
    "RuntimeStepOutputRepository",
    "SourceSignatureRepository",
]
