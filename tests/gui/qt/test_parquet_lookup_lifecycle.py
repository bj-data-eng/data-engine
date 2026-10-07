from __future__ import annotations

import threading

import openpyxl
import polars as pl
import pytest
from PySide6.QtCore import QThread, Signal

from data_engine.ui.gui.rendering import artifacts
from data_engine.ui.gui.rendering.preview_filters import ColumnFilter


@pytest.mark.parametrize("descending", [False, True])
@pytest.mark.parametrize("filtered", [False, True])
def test_folder_manifest_preview_and_export_match_reference(tmp_path, descending, filtered):
    nested = tmp_path / "nested"
    nested.mkdir()
    frames = (
        (tmp_path / "B.parquet", pl.DataFrame({"id": [9, 1], "label": ["B9", "B1"]})),
        (tmp_path / "a.parquet", pl.DataFrame({"id": [8, 3, 6], "label": ["a8", "a3", "a6"]})),
        (nested / "C.parquet", pl.DataFrame({"id": [2], "label": ["C2"]})),
    )
    for path, frame in frames:
        frame.write_parquet(path)
    source = tmp_path / "**" / "*.parquet"
    expression = pl.col("id") <= 6
    expected = pl.scan_parquet(source)
    if filtered:
        expected = expected.filter(expression)
    expected = expected.sort("id", descending=descending).head(3).collect()
    loader = artifacts._ParquetPreviewLoader(
        source,
        active_filters={"id": ColumnFilter.number_range("id", "", "6")} if filtered else {},
        sort_columns=(("id", descending),),
        preview_row_limit=3,
    )
    loaded, errors = [], []
    loader.preview_loaded.connect(lambda schema, preview, summary: loaded.append(preview))
    loader.load_failed.connect(errors.append)
    loader.run()
    assert errors == []
    assert loaded[0].equals(expected)
    output = tmp_path / "export.xlsx"
    artifacts.write_excel_atomic(loaded[0], output, worksheet="Preview")
    workbook = openpyxl.load_workbook(output, read_only=True)
    try:
        assert list(workbook["Preview"].values) == [tuple(expected.columns), *expected.rows()]
    finally:
        workbook.close()


def test_manifest_is_not_reglobbed_during_row_lookup(tmp_path, monkeypatch):
    first = tmp_path / "B.parquet"
    second = tmp_path / "a.parquet"
    pl.DataFrame({"id": [1]}).write_parquet(first)
    pl.DataFrame({"id": [2]}).write_parquet(second)
    calls = []
    original = artifacts._parquet_metadata_paths

    def manifest(path):
        calls.append(path)
        return original(path) if len(calls) == 1 else (second, first)

    monkeypatch.setattr(artifacts, "_parquet_metadata_paths", manifest)
    loader = artifacts._ParquetPreviewLoader(
        tmp_path / "*.parquet", active_filters={"id": ColumnFilter.distinct("id", (1,))},
        sort_columns=(), preview_row_limit=1,
    )
    loaded = []
    loader.preview_loaded.connect(lambda schema, preview, summary: loaded.append(preview))
    loader.run()
    assert loaded[0]["id"].to_list() == [1]
    assert len(calls) == 1


def test_clear_retains_running_queries_and_rejects_late_results(qapp, tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    distinct_entered = threading.Event()

    class BlockedPreview(QThread):
        preview_loaded = Signal(object, object, str)
        load_failed = Signal(str)

        def __init__(self, *args, **kwargs):
            super().__init__()

        def run(self):
            entered.set()
            release.wait(10)
            self.preview_loaded.emit(pl.Schema({"id": pl.Int64}), pl.DataFrame({"id": [1]}), "late")

    class BlockedDistinct(QThread):
        values_loaded = Signal(str, int, object, bool)
        load_failed = Signal(str, int, str)

        def run(self):
            distinct_entered.set()
            release.wait(10)
            self.values_loaded.emit("id", 1, [("1", 1)], False)

    monkeypatch.setattr(artifacts, "_ParquetPreviewLoader", BlockedPreview)
    widget = artifacts._ParquetExplorerWidget(tmp_path / "unused.parquet")
    worker = widget._preview_loader
    distinct_worker = BlockedDistinct()
    distinct_worker.values_loaded.connect(widget._handle_distinct_values_loaded)
    distinct_worker.load_failed.connect(widget._handle_distinct_values_failed)
    distinct_worker.finished.connect(lambda: widget._drop_distinct_loader(distinct_worker))
    widget._distinct_loaders.append(distinct_worker)
    distinct_worker.start()
    try:
        assert entered.wait(5)
        assert distinct_entered.wait(5)
        widget.shutdown_background_work()
        assert worker.isRunning()
        assert worker in artifacts._RETIRED_PARQUET_LOADERS
        assert distinct_worker.isRunning()
        assert distinct_worker in artifacts._RETIRED_PARQUET_LOADERS
        widget._handle_preview_loaded(None, pl.DataFrame({"id": [99]}), "stale queued result")
        widget._handle_preview_finished()
        assert widget._current_preview.is_empty()
        assert widget._preview_loader is None
        release.set()
        assert worker.wait(5000)
        assert distinct_worker.wait(5000)
        qapp.processEvents()
        assert worker not in artifacts._RETIRED_PARQUET_LOADERS
        assert distinct_worker not in artifacts._RETIRED_PARQUET_LOADERS
        assert widget.table.rowCount() == 0
    finally:
        release.set()
        worker.wait(5000)
        distinct_worker.wait(5000)
        widget.shutdown_background_work()
        widget.deleteLater()
        qapp.processEvents()


def test_background_query_panics_reach_ui_failure_signals(tmp_path, monkeypatch):
    pl.DataFrame({"id": [1]}).write_parquet(tmp_path / "input.parquet")

    def panic(*args, **kwargs):
        raise pl.exceptions.PanicException("native query failed")

    monkeypatch.setattr(artifacts.pl, "scan_parquet", panic)
    preview = artifacts._ParquetPreviewLoader(
        tmp_path / "input.parquet", active_filters={}, sort_columns=(), preview_row_limit=200,
    )
    distinct = artifacts._DistinctValueLoader(
        tmp_path / "input.parquet", "id", token=1, active_filters={}, value_filter=None,
        sort_descending=None, search_text="",
    )
    failures = []
    preview.load_failed.connect(failures.append)
    distinct.load_failed.connect(lambda name, token, message: failures.append(message))
    preview.run()
    distinct.run()
    assert failures == ["native query failed", "native query failed"]
