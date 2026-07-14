"""Shared SQLite connection plumbing for runtime state stores."""

from __future__ import annotations

from pathlib import Path
import sqlite3
import threading
import time

from data_engine.platform.paths import stable_absolute_path


class _RuntimeSqliteStore:
    """Own one SQLite-backed runtime store and expose narrow read/write helpers."""

    HISTORY_RETENTION_DAYS = 7
    _CONNECTION_CONFIGURATION_TIMEOUT_SECONDS = 5.0

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
                try:
                    connection.row_factory = sqlite3.Row
                    connection.execute("PRAGMA foreign_keys = ON")
                    connection.execute("PRAGMA busy_timeout = 5000")
                    self._enable_wal_with_retry(connection)
                    connection.execute("PRAGMA wal_autocheckpoint = 100")
                except BaseException:
                    connection.close()
                    raise
                self._connections[thread_id] = connection
            return connection

    def _enable_wal_with_retry(self, connection: sqlite3.Connection) -> None:
        """Enable WAL across concurrent first-open races without a process-local lock."""
        deadline = time.monotonic() + self._CONNECTION_CONFIGURATION_TIMEOUT_SECONDS
        while True:
            try:
                row = connection.execute("PRAGMA journal_mode = WAL").fetchone()
            except sqlite3.OperationalError as exc:
                error_code = getattr(exc, "sqlite_errorcode", None)
                primary_error_code = (
                    error_code & 0xFF if isinstance(error_code, int) else None
                )
                if primary_error_code not in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(remaining, 0.01))
                continue
            if row is None or str(row[0]).casefold() != "wal":
                raise sqlite3.OperationalError(
                    f"Unable to enable WAL mode for runtime database {self.db_path}."
                )
            return

    def close(self) -> None:
        """Close all SQLite connections opened for this store across threads."""
        with self._connections_lock:
            connections = tuple(self._connections.values())
            self._connections.clear()
        for connection in connections:
            connection.close()

    def close_current_thread_connection(self) -> None:
        """Close the SQLite connection owned by the current thread when present."""
        thread_id = threading.get_ident()
        with self._connections_lock:
            connection = self._connections.pop(thread_id, None)
        if connection is not None:
            connection.close()

    def __del__(self) -> None:
        """Best-effort cleanup for store connections when callers forget to close."""
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
