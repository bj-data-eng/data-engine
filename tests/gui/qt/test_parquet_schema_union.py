from __future__ import annotations

from datetime import datetime

import openpyxl
import polars as pl
import pytest

from data_engine.ui.gui.rendering import artifacts
from data_engine.ui.gui.rendering.preview_filters import ColumnFilter, NULL_FILTER_VALUE


@pytest.fixture
def mixed_schema_dataset(tmp_path):
    nested = tmp_path / "nested"
    nested.mkdir()
    frames = (
        (tmp_path / "A-empty.parquet", pl.DataFrame(schema={"id": pl.Int64})),
        (tmp_path / "B.parquet", pl.DataFrame({"id": [1, 2], "label": ["B1", "B2"]})),
        (tmp_path / "a.parquet", pl.DataFrame({
            "score": [40, 10], "id": [3, 4],
            "archived_at": pl.Series([datetime(2026, 1, 2), datetime(2026, 1, 3)], dtype=pl.Datetime("ms")),
        })),
        (nested / "C.parquet", pl.DataFrame({"id": [5], "label": ["C5"], "score": [20]})),
        (tmp_path / "z.parquet", pl.DataFrame({
            "id": [6], "late_column": ["z6"], "__preview_row_index": [100], "__preview_order": [200],
        })),
    )
    for path, frame in frames:
        frame.write_parquet(path)
    ordered = sorted(frames, key=lambda item: str(item[0]))
    expected = pl.concat([frame for _path, frame in ordered], how="diagonal")
    return tmp_path / "**" / "*.parquet", expected


def load_preview(source, *, filters=None, sort=(), limit=200):
    loader = artifacts._ParquetPreviewLoader(
        source, active_filters=filters or {}, sort_columns=sort, preview_row_limit=limit,
    )
    loaded, errors = [], []
    loader.preview_loaded.connect(lambda schema, frame, summary: loaded.append((schema, frame, summary)))
    loader.load_failed.connect(errors.append)
    loader.run()
    assert errors == []
    assert len(loaded) == 1
    return loaded[0]


@pytest.mark.parametrize("limit", [1, 200])
def test_initial_folder_load_unions_all_columns_even_outside_preview(mixed_schema_dataset, limit):
    source, expected = mixed_schema_dataset
    schema, preview, summary = load_preview(source, limit=limit)
    assert schema == expected.schema
    assert preview.equals(expected.head(limit))
    assert summary == f"6 rows - {expected.width} columns - showing top {limit} rows"
    assert schema["archived_at"] == pl.Datetime("ms")


@pytest.mark.parametrize("column", ["id", "score", "late_column"])
@pytest.mark.parametrize("descending", [False, True])
def test_sorted_preview_aligns_file_local_rows_to_union_schema(mixed_schema_dataset, column, descending):
    source, expected = mixed_schema_dataset
    sort = ((column, descending),) if column == "id" else ((column, descending), ("id", False))
    reference = expected.sort([name for name, _desc in sort], descending=[desc for _name, desc in sort]).head(4)
    schema, preview, summary = load_preview(source, sort=sort, limit=4)
    assert schema == expected.schema
    assert preview.equals(reference)
    assert summary.startswith("6 rows - ")


@pytest.mark.parametrize("column_filter,expression", [
    (ColumnFilter.number_range("score", "15", "40"), pl.col("score").is_between(15, 40)),
    (ColumnFilter.distinct("score", (NULL_FILTER_VALUE,)), pl.col("score").is_null()),
    (ColumnFilter.text("late_column", "contains", "z"), pl.col("late_column").str.contains("z", literal=True)),
    (ColumnFilter.date_range("archived_at", "2026-01-03", "2026-01-03"), pl.col("archived_at").dt.date() == datetime(2026, 1, 3).date()),
    (ColumnFilter.number_range("score", "100", "200"), pl.col("score").is_between(100, 200)),
])
@pytest.mark.parametrize("sorted_preview", [False, True])
def test_filtering_optional_columns_preserves_missing_rows_as_typed_nulls(
    mixed_schema_dataset, column_filter, expression, sorted_preview,
):
    source, reference = mixed_schema_dataset
    reference = reference.filter(expression)
    if sorted_preview:
        reference = reference.sort("id", descending=True)
    schema, preview, summary = load_preview(
        source, filters={column_filter.column_name: column_filter},
        sort=(("id", True),) if sorted_preview else (), limit=2,
    )
    assert preview.equals(reference.head(2))
    assert preview.schema == schema
    assert summary.startswith(f"{reference.height} rows - ")


@pytest.mark.parametrize("filtered", [False, True])
def test_filter_value_list_includes_optional_column_values_and_blanks(mixed_schema_dataset, filtered):
    source, _expected = mixed_schema_dataset
    loader = artifacts._DistinctValueLoader(
        source, "score", token=7,
        active_filters={"id": ColumnFilter.number_range("id", "3", "5")} if filtered else {},
        value_filter=None, sort_descending=False, search_text="",
    )
    loaded, errors = [], []
    loader.values_loaded.connect(lambda name, token, values, truncated: loaded.append((name, token, values, truncated)))
    loader.load_failed.connect(lambda name, token, message: errors.append(message))
    loader.run()
    assert errors == []
    name, token, values, truncated = loaded[0]
    expected = [("10", 10), ("20", 20), ("40", 40)]
    if not filtered:
        expected.insert(0, ("(blank)", NULL_FILTER_VALUE))
    assert values == expected
    assert (name, token, truncated) == ("score", 7, False)


def test_export_preserves_union_columns_and_missing_values(mixed_schema_dataset, tmp_path):
    source, expected = mixed_schema_dataset
    _schema, preview, _summary = load_preview(source, sort=(("id", True),), limit=4)
    expected = expected.sort("id", descending=True).head(4)
    output = tmp_path / "preview.xlsx"
    artifacts.write_excel_atomic(preview, output, worksheet="Preview")
    workbook = openpyxl.load_workbook(output, read_only=True)
    try:
        assert list(workbook["Preview"].values) == [tuple(expected.columns), *expected.rows()]
    finally:
        workbook.close()


def test_union_schema_discovery_reads_metadata_without_materializing_files(mixed_schema_dataset, monkeypatch):
    source, expected = mixed_schema_dataset

    def forbid_full_read(*args, **kwargs):
        pytest.fail("Dataset schema discovery must not materialize Parquet files")

    original_top = artifacts._sorted_top_parquet_preview

    def check_key_projection(query, sort_columns, row_limit):
        assert query.collect_schema().names() == ["__preview_row_index_1", "score", "id"]
        return original_top(query, sort_columns, row_limit)

    monkeypatch.setattr(pl, "read_parquet", forbid_full_read)
    monkeypatch.setattr(artifacts, "_sorted_top_parquet_preview", check_key_projection)
    _schema, preview, _summary = load_preview(source, sort=(("score", False), ("id", False)), limit=2)
    assert preview.equals(expected.sort(["score", "id"]).head(2))


def test_shared_column_type_conflicts_fail_clearly_instead_of_dropping_data(tmp_path):
    pl.DataFrame({"id": [1]}).write_parquet(tmp_path / "a.parquet")
    pl.DataFrame({"id": ["two"], "extra": [2]}).write_parquet(tmp_path / "b.parquet")
    loader = artifacts._ParquetPreviewLoader(
        tmp_path / "*.parquet", active_filters={}, sort_columns=(), preview_row_limit=200,
    )
    loaded, errors = [], []
    loader.preview_loaded.connect(lambda *args: loaded.append(args))
    loader.load_failed.connect(errors.append)
    loader.run()
    assert loaded == []
    assert len(errors) == 1
    assert "'id' has conflicting types: Int64 and String" in errors[0]
    assert "b.parquet" in errors[0]


def test_single_file_still_preserves_its_schema_and_column_order(tmp_path):
    path = tmp_path / "single.parquet"
    expected = pl.DataFrame({"note": ["a", "b"], "id": [2, 1]})
    expected.write_parquet(path)
    schema, preview, _summary = load_preview(path)
    assert schema == expected.schema
    assert preview.equals(expected)
