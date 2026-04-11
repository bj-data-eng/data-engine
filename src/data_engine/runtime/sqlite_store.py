"""Shared SQLite connection plumbing for runtime state stores."""

from __future__ import annotations

from pathlib import Path
import sqlite3
import threading

from data_engine.platform.paths import stable_absolute_path


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
        """Close all SQLite connections opened for this store across threads."""
        with self._connections_lock:
            connections = tuple(self._connections.values())
            self._connections.clear()
        for connection in connections:
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
