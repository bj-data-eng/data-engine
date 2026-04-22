from __future__ import annotations

from pathlib import Path

import pytest

from data_engine.core.helpers import (
    _callable_identifier,
    _callable_name,
    _normalize_extensions,
    _normalize_watch_times,
    _parse_duration,
    _parse_schedule_at,
    _resolve_flow_path,
    _title_case_words,
    _validate_label,
    _validate_slot_name,
)
from data_engine.core.model import FlowValidationError
from data_engine.flow_modules.flow_module_loader import compiled_flow_module_context


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("10ms", 0.01),
        ("2s", 2.0),
        ("3m", 180.0),
        ("1.5h", 5400.0),
        ("2d", 172800.0),
        ("1w", 604800.0),
    ],
)
def test_parse_duration_supports_all_units(raw: str, expected: float):
    assert _parse_duration(raw) == expected


@pytest.mark.parametrize("raw", ["0s", "-1m", "abc", "5x"])
def test_parse_duration_rejects_invalid_values(raw: str):
    with pytest.raises(FlowValidationError):
        _parse_duration(raw)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("00:00", (0, 0)),
        ("09:31", (9, 31)),
        ("23:59", (23, 59)),
    ],
)
def test_parse_schedule_at_validates_hh_mm(raw: str, expected: tuple[int, int]):
    assert _parse_schedule_at(raw) == expected


@pytest.mark.parametrize("raw", ["9:31", "24:00", "12:60", "bad"])
def test_parse_schedule_at_rejects_invalid_values(raw: str):
    with pytest.raises(FlowValidationError):
        _parse_schedule_at(raw)


def test_normalize_watch_times_dedupes_and_sorts_slots():
    assert _normalize_watch_times(("14:45", "08:15", "14:45")) == ("08:15", "14:45")


def test_normalize_watch_times_accepts_single_string_and_rejects_invalid_types():
    assert _normalize_watch_times("08:15") == ("08:15",)

    with pytest.raises(FlowValidationError):
        _normalize_watch_times([])

    with pytest.raises(FlowValidationError):
        _normalize_watch_times(object())


def test_normalize_extensions_normalizes_prefixes_and_handles_empty_values():
    assert _normalize_extensions(None) is None
    assert _normalize_extensions(("XLSX", ".Csv")) == (".xlsx", ".csv")
    assert _normalize_extensions(["parquet", "json"]) == (".parquet", ".json")

    with pytest.raises(FlowValidationError):
        _normalize_extensions([])

    with pytest.raises(FlowValidationError):
        _normalize_extensions([".xlsx", ""])


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", "Step"),
        ("simple_name", "Simple Name"),
        ("alreadyTitleCase", "Already Title Case"),
        ("  multiple_words_here  ", "Multiple Words Here"),
    ],
)
def test_title_case_words_normalizes_common_name_shapes(raw: str, expected: str):
    assert _title_case_words(raw) == expected


def test_callable_name_and_identifier_cover_functions_classes_lambdas_and_instances():
    def build_docs_summary():
        return None

    class DocsCleaner:
        def __call__(self):
            return None

    class ExplicitClass:
        pass

    instance = DocsCleaner()
    lambda_fn = lambda: None  # noqa: E731

    assert _callable_name(build_docs_summary) == "Build Docs Summary"
    assert _callable_identifier(build_docs_summary) == "build_docs_summary"
    assert _callable_name(ExplicitClass) == "Explicit Class"
    assert _callable_identifier(ExplicitClass) == "ExplicitClass"
    assert _callable_name(instance) == "Docs Cleaner"
    assert _callable_identifier(instance) == "DocsCleaner"
    assert _callable_name(lambda_fn) == "Lambda"
    assert _callable_identifier(lambda_fn) == "<lambda>"


def test_resolve_flow_path_handles_absolute_and_compiled_relative_paths(tmp_path: Path):
    absolute = (tmp_path / "absolute" / "flow.py").resolve()
    compiled_root = tmp_path / "compiled"
    relative = Path("nested/flow.py")

    assert _resolve_flow_path(absolute) == absolute
    assert _resolve_flow_path(relative) == relative.resolve()

    compiled_root.mkdir()
    with compiled_flow_module_context(compiled_root):
        assert _resolve_flow_path(relative) == (compiled_root / relative).resolve()


@pytest.mark.parametrize(
    ("slot_name", "value", "expected"),
    [
        ("source", None, None),
        ("source", "  current ", "current"),
        ("save_as", "filtered", "filtered"),
    ],
)
def test_validate_slot_name_normalizes_and_allows_expected_values(slot_name: str, value: str | None, expected: str | None):
    assert _validate_slot_name(method_name="step", slot_name=slot_name, value=value) == expected


@pytest.mark.parametrize(
    ("slot_name", "value"),
    [
        ("source", ""),
        ("source", "   "),
        ("source", 1),
        ("save_as", "current"),
    ],
)
def test_validate_slot_name_rejects_invalid_values(slot_name: str, value):
    with pytest.raises(FlowValidationError):
        _validate_slot_name(method_name="step", slot_name=slot_name, value=value)


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        (None, None),
        ("  Keep Current  ", "Keep Current"),
    ],
)
def test_validate_label_normalizes_valid_values(label: str | None, expected: str | None):
    assert _validate_label(method_name="step", label=label) == expected


@pytest.mark.parametrize("label", ["", "   ", 1])
def test_validate_label_rejects_invalid_values(label):
    with pytest.raises(FlowValidationError):
        _validate_label(method_name="step", label=label)


