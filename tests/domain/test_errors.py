from __future__ import annotations

import pytest

from data_engine.domain import StructuredErrorState


@pytest.mark.parametrize(
    "prefix",
    [
        'Flow "docs" failed in step "Transform" (function transform): ',
        'Flow module "docs" failed during build() in build: ',
        'Flow module "docs" failed during import: ',
    ],
)
def test_structured_error_preserves_multiline_polars_detail(prefix):
    detail = 'ColumnNotFoundError: missing\n\nResolved plan until failure:\n\t---> FAILED HERE <---\n'
    text = prefix + detail

    parsed = StructuredErrorState.parse(text)

    assert parsed is not None
    assert parsed.detail == detail
    assert parsed.raw_text == text


def test_structured_step_error_separates_helper_traceback_from_error_summary():
    detail = 'ComputeError: invalid cast\n\nResolved plan until failure:\n  SELECT\n'
    traceback = 'Traceback (most recent call last):\n  File "helpers.py", line 12, in transform\nComputeError: invalid cast'
    text = f'Flow "docs" failed in step "Transform": {detail}\n\nPython traceback:\n{traceback}'

    parsed = StructuredErrorState.parse(text)

    assert parsed is not None
    assert parsed.detail == detail
    assert parsed.raw_text == text
