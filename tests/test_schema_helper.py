from __future__ import annotations

from datetime import time

import polars as pl
import pytest

from data_engine.helpers import TableSchema
from data_engine.helpers import normalize_column_name
from data_engine.helpers import normalize_column_names
from data_engine.helpers import normalized_column_renames


def test_table_schema_exposes_polars_chain_parts():
    schema = TableSchema(
        dtypes={
            "step_to": pl.String,
            "time": pl.Time,
        },
        rename={
            "step_to": "step",
            "workflow_to": "workflow",
        },
        drop=("workflow_from", "ssn"),
    )

    assert schema.columns == ("step_to", "time", "workflow_to", "workflow_from", "ssn")
    assert schema.drop == ("workflow_from", "ssn")
    assert schema.rename == {"step_to": "step", "workflow_to": "workflow"}


def test_table_schema_parts_select_cast_drop_and_rename():
    schema = TableSchema(
        dtypes={
            "step_to": pl.String,
            "time": pl.Time,
        },
        rename={
            "step_to": "step",
            "workflow_to": "workflow",
        },
        drop=("workflow_from", "ssn"),
    )
    df = pl.DataFrame(
        {
            "step_to": [1],
            "time": [time(10, 30)],
            "workflow_to": ["claims"],
            "workflow_from": ["legacy"],
            "ssn": ["123-45-6789"],
            "ignored": ["not selected"],
        }
    )

    result = schema.columns.apply(df)
    result = schema.dtypes.apply(result)
    result = schema.drop.apply(result)
    result = schema.rename.apply(result)

    assert result.columns == ["step", "time", "workflow"]
    assert result.schema["step"] == pl.String
    assert result.schema["time"] == pl.Time
    assert result.to_dict(as_series=False) == {
        "step": ["1"],
        "time": [time(10, 30)],
        "workflow": ["claims"],
    }


def test_table_schema_attributes_apply_individually():
    schema = TableSchema(
        dtypes={"step_to": pl.String},
        rename={"step_to": "step"},
        drop=("ssn",),
    )
    df = pl.DataFrame({"step_to": [1], "ssn": ["123-45-6789"]})

    result = schema.columns.apply(df)
    result = schema.dtypes.apply(result)
    result = schema.drop.apply(result)
    result = schema.rename.apply(result)

    assert result.to_dict(as_series=False) == {"step": ["1"]}


def test_table_schema_parts_preserve_lazy_frames():
    schema = TableSchema(
        dtypes={"step_to": pl.String},
        rename={"step_to": "step"},
        drop=("ssn",),
    )
    lazy_df = pl.DataFrame({"step_to": [1], "ssn": ["123-45-6789"]}).lazy()

    result = schema.columns.apply(lazy_df)
    result = schema.dtypes.apply(result)
    result = schema.drop.apply(result)
    result = schema.rename.apply(result)

    assert isinstance(result, pl.LazyFrame)
    assert result.collect().to_dict(as_series=False) == {"step": ["1"]}


def test_table_schema_rejects_empty_column_names():
    with pytest.raises(ValueError, match="non-empty"):
        TableSchema(dtypes={"": pl.String})


def test_normalize_column_name_collapses_spaces_and_lowercases():
    assert normalize_column_name("  Step   To  ") == "step_to"
    assert normalize_column_name("Workflow\tTo") == "workflow_to"
    assert normalize_column_name("Already_Normal") == "already_normal"


def test_normalized_column_renames_only_returns_changed_columns():
    assert normalized_column_renames(["Step   To", "already_normal"]) == {"Step   To": "step_to"}


def test_normalize_column_names_renames_eager_and_lazy_frames():
    df = pl.DataFrame({"Step   To": [1], "Workflow To": ["claims"]})
    eager = normalize_column_names(df)
    lazy = normalize_column_names(df.lazy())

    assert eager.columns == ["step_to", "workflow_to"]
    assert lazy.collect().columns == ["step_to", "workflow_to"]


def test_table_schema_can_normalize_column_names():
    schema = TableSchema()
    df = pl.DataFrame({"Step   To": [1], "Workflow To": ["claims"]})

    result = schema.normalize_column_names(df)

    assert result.columns == ["step_to", "workflow_to"]


def test_column_selection_can_normalize_specified_column_names():
    schema = TableSchema(
        dtypes={"Step   To": pl.Int64},
        rename={"Workflow To": "workflow"},
    )
    df = pl.DataFrame({"Step   To": [1], "Workflow To": ["claims"], "Other Column": ["left alone"]})

    result = schema.columns.normalize_column_names(df)

    assert result.columns == ["step_to", "workflow_to", "Other Column"]
