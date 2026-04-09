"""Machine-local settings services."""

from __future__ import annotations

from pathlib import Path
from typing import Callable

from data_engine.platform.local_settings import LocalSettingsStore, default_settings_db_path


class SettingsService:
    """Own machine-local settings persistence for operator surfaces."""

    def __init__(self, store: LocalSettingsStore) -> None:
        self._store = store

    @classmethod
    def default_store(cls, *, app_root: Path | None = None) -> LocalSettingsStore:
        """Open the default local settings store for one app root."""
        return LocalSettingsStore(default_settings_db_path(app_root=app_root))

    @classmethod
    def open_default(
        cls,
        *,
        app_root: Path | None = None,
        store_factory: Callable[[Path | None], LocalSettingsStore] | None = None,
    ) -> "SettingsService":
        """Open the default local settings store for the current app root."""
        store_factory = store_factory or (lambda root: cls.default_store(app_root=root))
        return cls(store_factory(app_root))

    def workspace_collection_root(self) -> Path | None:
        """Return the saved local workspace collection root override, when present."""
        return self._store.workspace_collection_root()

    def set_workspace_collection_root(self, value: Path | str | None) -> None:
        """Persist the local workspace collection root override."""
        self._store.set_workspace_collection_root(value)

    def default_workspace_id(self) -> str | None:
        """Return the saved default workspace id, when present."""
        return self._store.default_workspace_id()

    def set_default_workspace_id(self, value: str | None) -> None:
        """Persist the default workspace id."""
        self._store.set_default_workspace_id(value)

    def runtime_root(self) -> Path | None:
        """Return the saved runtime/artifact root override, when present."""
        return self._store.runtime_root()

    def set_runtime_root(self, value: Path | str | None) -> None:
        """Persist the runtime/artifact root override."""
        self._store.set_runtime_root(value)


__all__ = ["SettingsService"]
