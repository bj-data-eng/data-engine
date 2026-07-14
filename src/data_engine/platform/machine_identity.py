"""Durable installation identity and human-readable host metadata."""

from __future__ import annotations

from pathlib import Path
import socket
from threading import RLock

from data_engine.platform.local_settings import LocalSettingsStore, default_settings_db_path
from data_engine.platform.paths import stable_absolute_path


_MACHINE_ID_CACHE: dict[Path, str] = {}
_MACHINE_ID_CACHE_LOCK = RLock()


def machine_id_text(
    *,
    app_root: Path | None = None,
    settings_path: Path | None = None,
) -> str:
    """Return the durable UUID for one machine-local Data Engine installation.

    Args:
        app_root: Application root whose default local settings store owns the
            identity.
        settings_path: Explicit settings database path. This is mutually
            exclusive with ``app_root``.

    Returns:
        A canonical version-4 UUID string. The value is cached by resolved
        settings path for the life of the process after its first durable read.

    Raises:
        ValueError: If both path-selection arguments are supplied.
        OSError: If the identity cannot be persisted safely.
        sqlite3.Error: If the settings database cannot be read or updated.
    """
    if app_root is not None and settings_path is not None:
        raise ValueError("Pass either app_root or settings_path, not both.")
    resolved_settings_path = stable_absolute_path(
        settings_path if settings_path is not None else default_settings_db_path(app_root=app_root)
    )
    with _MACHINE_ID_CACHE_LOCK:
        cached = _MACHINE_ID_CACHE.get(resolved_settings_path)
        if cached is not None:
            return cached
        machine_id = LocalSettingsStore(resolved_settings_path).installation_id()
        _MACHINE_ID_CACHE[resolved_settings_path] = machine_id
        return machine_id


def host_name_text() -> str:
    """Return the current hostname for display-only machine metadata."""
    return socket.gethostname().strip() or "unknown-host"


__all__ = ["host_name_text", "machine_id_text"]
