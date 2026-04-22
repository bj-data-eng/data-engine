from __future__ import annotations

from pathlib import Path

import pytest

from data_engine.core.model import FlowValidationError
import data_engine.runtime.file_watch as file_watch
from data_engine.runtime.file_watch import PollingWatcher, is_temporary_file_path, iter_candidate_paths

from tests.services.support import rewrite_with_new_timestamp


def test_iter_candidate_paths_filters_temp_files_and_extensions(tmp_path):
    (tmp_path / "~$draft.xlsx").write_text("x", encoding="utf-8")
    (tmp_path / ".hidden.xlsx").write_text("x", encoding="utf-8")
    (tmp_path / "notes.csv").write_text("x", encoding="utf-8")
    good = tmp_path / "docs.xlsx"
    good.write_text("x", encoding="utf-8")

    paths = list(iter_candidate_paths(tmp_path, extensions=(".xlsx",)))

    assert paths == [good]


def test_iter_candidate_paths_respects_non_recursive_and_single_file(tmp_path):
    nested = tmp_path / "nested"
    nested.mkdir()
    top = tmp_path / "top.xlsx"
    deep = nested / "deep.xlsx"
    top.write_text("x", encoding="utf-8")
    deep.write_text("x", encoding="utf-8")

    assert list(iter_candidate_paths(tmp_path, extensions=(".xlsx",), recursive=False)) == [top]
    assert list(iter_candidate_paths(top, extensions=(".xlsx",))) == [top]


def test_iter_candidate_paths_raises_for_missing_root(tmp_path):
    with pytest.raises(FlowValidationError, match="Input root not found"):
        list(iter_candidate_paths(tmp_path / "missing"))


def test_iter_candidate_paths_can_tolerate_missing_root_when_requested(tmp_path):
    assert list(iter_candidate_paths(tmp_path / "missing", allow_missing=True)) == []


def test_iter_candidate_paths_does_not_round_trip_through_path_constructor(tmp_path, monkeypatch):
    left = tmp_path / "alpha" / "docs.xlsx"
    right = tmp_path / "beta" / "docs.xlsx"
    left.parent.mkdir(parents=True)
    right.parent.mkdir(parents=True)
    left.write_text("x", encoding="utf-8")
    right.write_text("x", encoding="utf-8")

    def _boom(*args, **kwargs):  # pragma: no cover - defensive test hook
        raise AssertionError("Path constructor should not be used while sorting candidate paths")

    monkeypatch.setattr(file_watch, "Path", _boom)

    assert list(iter_candidate_paths(tmp_path, extensions=(".xlsx",))) == [left, right]


def test_temporary_file_helper_covers_common_transient_patterns():
    assert is_temporary_file_path(Path(".~lock.report.xlsx#")) is True
    assert is_temporary_file_path(Path("report.xlsx~")) is True
    assert is_temporary_file_path(Path("report.xlsx.download")) is True
    assert is_temporary_file_path(Path("report.xlsx")) is False


def test_polling_watcher_detects_new_and_modified_files_after_settle(tmp_path):
    watcher = PollingWatcher(tmp_path, extensions=(".xlsx",), settle=1)
    watcher.start()

    created = tmp_path / "docs.xlsx"
    created.write_text("v1", encoding="utf-8")

    assert watcher.drain_events() == []
    assert watcher.drain_events() == [created]

    rewrite_with_new_timestamp(created, "v2")

    assert watcher.drain_events() == []
    assert watcher.drain_events() == [created]


def test_polling_watcher_supports_single_file_roots_and_stop(tmp_path):
    target = tmp_path / "docs.xlsx"
    target.write_text("v1", encoding="utf-8")

    watcher = PollingWatcher(target, settle=0)
    watcher.start()
    rewrite_with_new_timestamp(target, "v2")
    assert watcher.drain_events() == [target]

    watcher.stop()
    rewrite_with_new_timestamp(target, "v3")
    assert watcher.drain_events() == []


def test_polling_watcher_ignores_preexisting_file_on_start(tmp_path):
    existing = tmp_path / "docs.xlsx"
    existing.write_text("v1", encoding="utf-8")

    watcher = PollingWatcher(tmp_path, extensions=(".xlsx",), settle=1)
    watcher.start()

    assert watcher.drain_events() == []


def test_polling_watcher_reprocesses_deleted_then_recreated_file(tmp_path):
    target = tmp_path / "docs.xlsx"
    target.write_text("v1", encoding="utf-8")

    watcher = PollingWatcher(tmp_path, extensions=(".xlsx",), settle=1)
    watcher.start()

    target.unlink()
    assert watcher.drain_events() == []

    rewrite_with_new_timestamp(target, "v2")
    assert watcher.drain_events() == []
    assert watcher.drain_events() == [target]


def test_polling_watcher_can_start_before_single_file_exists(tmp_path):
    target = tmp_path / "docs.xlsx"

    watcher = PollingWatcher(target, settle=0)
    watcher.start()

    target.write_text("v1", encoding="utf-8")

    assert watcher.drain_events() == [target]


def test_polling_watcher_rejects_negative_settle(tmp_path):
    with pytest.raises(FlowValidationError, match="zero or greater"):
        PollingWatcher(tmp_path, settle=-1)

