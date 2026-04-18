"""Machine-local app settings persisted in a local SQLite database."""

from __future__ import annotations

import os
from pathlib import Path
import sqlite3

from data_engine.platform.identity import APP_CACHE_DIR_NAME, env_var
from data_engine.platform.paths import stable_absolute_path


DATA_ENGINE_APP_ROOT_ENV_VAR = env_var("app_root")
DATA_ENGINE_STATE_ROOT_ENV_VAR = env_var("state_root")


def default_state_root(*, app_root: Path | None = None) -> Path:
    """Return the platform-local mutable state root for the app."""
    env_value = os.environ.get(DATA_ENGINE_STATE_ROOT_ENV_VAR)
    if env_value and env_value.strip():
        return stable_absolute_path(env_value)

    # Non-default app roots are used heavily by tests and isolated local runs.
    # Keep the real application on the normal platform-local state path, but
    # give explicit alternate roots an app-local state area so they do not
    # pollute the user's primary settings store.
    if app_root is not None:
        resolved_app_root = stable_absolute_path(app_root)
        from data_engine.platform.workspace_models import APP_ROOT_PATH

        if resolved_app_root != APP_ROOT_PATH:
            return resolved_app_root / ".data_engine_state"

    if sys_platform() == "darwin":
        home = Path(os.environ.get("HOME") or Path.home())
        return home / "Library" / "Application Support" / APP_CACHE_DIR_NAME
    home = Path.home()
    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA") or home / "AppData" / "Local")
        return base / APP_CACHE_DIR_NAME
    xdg_state = os.environ.get("XDG_STATE_HOME")
    if xdg_state and xdg_state.strip():
        return stable_absolute_path(xdg_state) / APP_CACHE_DIR_NAME
    xdg_data = os.environ.get("XDG_DATA_HOME")
    if xdg_data and xdg_data.strip():
        return stable_absolute_path(xdg_data) / APP_CACHE_DIR_NAME
    return home / ".local" / "share" / APP_CACHE_DIR_NAME


def default_settings_db_path(*, app_root: Path | None = None) -> Path:
    """Return the default machine-local settings database path."""
    return default_state_root(app_root=app_root) / "settings" / "app_settings.sqlite"


def sys_platform() -> str:
    """Return the normalized platform identifier."""
    import sys

    return sys.platform


class LocalSettingsStore:
    """Persist simple machine-local UI settings in SQLite."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = stable_absolute_path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @classmethod
    def open_default(cls, *, app_root: Path | None = None) -> "LocalSettingsStore":
        return cls(default_settings_db_path(app_root=app_root))

    def _connection(self) -> sqlite3.Connection:
        # Recreate the parent on every open so repeated temp-root churn during
        # long test sessions cannot strand the settings store on a deleted path.
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        self._ensure_schema(connection)
        return connection

    @staticmethod
    def _ensure_schema(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )

    def _initialize(self) -> None:
        connection = self._connection()
        connection.close()

    def get(self, key: str) -> str | None:
        connection = self._connection()
        try:
            row = connection.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        finally:
            connection.close()
        if row is None:
            return None
        value = row["value"]
        return str(value) if value is not None else None

    def set(self, key: str, value: str | None) -> None:
        connection = self._connection()
        try:
            if value is None or not str(value).strip():
                connection.execute("DELETE FROM settings WHERE key = ?", (key,))
            else:
                connection.execute(
                    """
                    INSERT INTO settings (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (key, str(value)),
                )
            connection.commit()
        finally:
            connection.close()

    def workspace_collection_root(self) -> Path | None:
        value = self.get("workspace_collection_root")
        if value is None or not value.strip():
            return None
        return stable_absolute_path(value)

    def set_workspace_collection_root(self, value: Path | str | None) -> None:
        if value is None:
            self.set("workspace_collection_root", None)
            return
        self.set("workspace_collection_root", str(stable_absolute_path(value)))

    def default_workspace_id(self) -> str | None:
        value = self.get("default_workspace_id")
        if value is None or not value.strip():
            return None
        return value.strip()

    def set_default_workspace_id(self, value: str | None) -> None:
        self.set("default_workspace_id", value.strip() if value is not None else None)

    def runtime_root(self) -> Path | None:
        value = self.get("runtime_root")
        if value is None or not value.strip():
            return None
        return stable_absolute_path(value)

    def set_runtime_root(self, value: Path | str | None) -> None:
        if value is None:
            self.set("runtime_root", None)
            return
        self.set("runtime_root", str(stable_absolute_path(value)))


__all__ = [
    "DATA_ENGINE_STATE_ROOT_ENV_VAR",
    "LocalSettingsStore",
    "default_settings_db_path",
    "default_state_root",
]
