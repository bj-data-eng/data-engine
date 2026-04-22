from __future__ import annotations

from data_engine.domain import StructuredErrorState


def test_structured_error_state_parses_step_failure_into_named_fields():
    parsed = StructuredErrorState.parse(
        'Flow "docs_summary" failed in step "Combine Docs" (function combine_docs) '
        'for source "/tmp/input.xlsx": ValueError: boom'
    )

    assert parsed is not None
    assert parsed.title == "Flow Failed"
    assert [(field.label, field.value) for field in parsed.fields] == [
        ("Flow", "docs_summary"),
        ("Phase", "step"),
        ("Step", "Combine Docs"),
        ("Function", "combine_docs"),
        ("Source", "/tmp/input.xlsx"),
    ]
    assert parsed.detail == "ValueError: boom"


def test_structured_error_state_returns_none_for_unstructured_text():
    assert StructuredErrorState.parse("plain warning text") is None

