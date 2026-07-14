from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from pathlib import Path
from uuid import UUID

import pytest

from data_engine.platform import machine_identity
from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.platform.machine_identity import host_name_text, machine_id_text


def _read_machine_id_in_child(settings_path: str) -> str:
    return machine_id_text(settings_path=Path(settings_path))


def test_machine_id_is_stable_across_settings_store_reopen(tmp_path):
    settings_path = tmp_path / "settings" / "app_settings.sqlite"

    first = LocalSettingsStore(settings_path).installation_id()
    second = LocalSettingsStore(settings_path).installation_id()

    assert second == first
    assert UUID(first).version == 4


def test_machine_id_cache_is_scoped_by_resolved_settings_path(tmp_path, monkeypatch):
    first_path = tmp_path / "first" / "app_settings.sqlite"
    second_path = tmp_path / "second" / "app_settings.sqlite"
    monkeypatch.setattr(machine_identity.socket, "gethostname", lambda: "cloned-host")

    first = machine_id_text(settings_path=first_path)
    second = machine_id_text(settings_path=second_path)

    assert first != second
    assert host_name_text() == "cloned-host"

    def _unexpected_store_read(_store):
        raise AssertionError("cached identity reopened SQLite")

    monkeypatch.setattr(LocalSettingsStore, "installation_id", _unexpected_store_read)
    assert machine_id_text(settings_path=first_path.resolve()) == first
    assert machine_id_text(settings_path=second_path.resolve()) == second


def test_concurrent_first_creation_converges_on_one_machine_id(tmp_path):
    settings_path = tmp_path / "concurrent" / "app_settings.sqlite"
    context = multiprocessing.get_context("spawn")

    with ProcessPoolExecutor(max_workers=4, mp_context=context) as executor:
        identities = tuple(
            executor.map(
                _read_machine_id_in_child,
                (str(settings_path),) * 8,
            )
        )

    assert len(set(identities)) == 1
    assert UUID(identities[0]).version == 4
    assert LocalSettingsStore(settings_path).installation_id() == identities[0]


def test_malformed_machine_id_is_repaired_and_persisted(tmp_path):
    settings_path = tmp_path / "malformed" / "app_settings.sqlite"
    store = LocalSettingsStore(settings_path)
    store.set("installation_id", "copied-hostname")

    repaired = machine_id_text(settings_path=settings_path)

    assert UUID(repaired).version == 4
    assert repaired != "copied-hostname"
    assert LocalSettingsStore(settings_path).get("installation_id") == repaired


def test_machine_id_fails_closed_when_identity_cannot_be_persisted(tmp_path, monkeypatch):
    settings_path = tmp_path / "unwritable" / "app_settings.sqlite"

    def _fail_persistence(_store):
        raise OSError("settings unavailable")

    monkeypatch.setattr(LocalSettingsStore, "installation_id", _fail_persistence)

    with pytest.raises(OSError, match="settings unavailable"):
        machine_id_text(settings_path=settings_path)


def test_machine_id_rejects_ambiguous_store_selection(tmp_path):
    with pytest.raises(ValueError, match="either app_root or settings_path"):
        machine_id_text(
            app_root=tmp_path / "app",
            settings_path=tmp_path / "settings.sqlite",
        )
