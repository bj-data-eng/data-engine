from __future__ import annotations

import logging

import pytest

from data_engine.domain.logs import (
    FlowLogEntry,
    RuntimeStepEvent,
    format_log_line,
    format_runtime_message,
    parse_runtime_event,
    parse_runtime_message,
    short_source_label,
)


def test_short_source_label_collapses_empty_and_path_values():
    assert short_source_label(None) == "-"
    assert short_source_label("") == "-"
    assert short_source_label("None") == "-"
    assert short_source_label("C:/input/claims.xlsx") == "claims.xlsx"
    assert short_source_label("relative/path/report.parquet") == "report.parquet"


def test_format_runtime_message_formats_step_flow_and_fallback_messages():
    step_message = "run=run-1 flow=poller step=Read Claims source=C:/input/claims.xlsx status=success elapsed=0.25"
    flow_message = "run=run-2 flow=poller source=C:/input/report.xlsx status=started"
    fallback_message = "see /tmp/input/claims.xlsx and /var/log/app.log"

    assert format_runtime_message(step_message) == "poller  Read Claims  success  claims.xlsx"
    assert format_runtime_message(flow_message) == "poller  started  report.xlsx"
    assert format_runtime_message(fallback_message) == "see claims.xlsx and app.log"


def test_format_log_line_uses_record_message():
    record = logging.makeLogRecord(
        {
            "msg": "run=run-3 flow=poller step=Write Summary source=/tmp/output/summary.xlsx status=success",
        }
    )

    assert format_log_line(record) == "poller  Write Summary  success  summary.xlsx"


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        (
            "run=run-1 flow=poller step=Read Claims source=C:/input/claims.xlsx status=success elapsed=0.25",
            RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name="Read Claims",
                source_label="claims.xlsx",
                status="success",
                elapsed_seconds=0.25,
            ),
        ),
        (
            "run=run-2 flow=poller source=C:/input/report.xlsx status=started",
            RuntimeStepEvent(
                run_id="run-2",
                flow_name="poller",
                step_name=None,
                source_label="report.xlsx",
                status="started",
                elapsed_seconds=None,
            ),
        ),
    ],
)
def test_parse_runtime_message_parses_step_and_flow_messages(message: str, expected: RuntimeStepEvent):
    assert parse_runtime_message(message) == expected


def test_parse_runtime_message_returns_none_for_unstructured_text():
    assert parse_runtime_message("plain text only") is None


def test_parse_runtime_event_wraps_log_record():
    record = logging.makeLogRecord(
        {
            "msg": "run=run-9 flow=poller step=Build Summary source=C:/output/summary.xlsx status=failed",
        }
    )

    assert parse_runtime_event(record) == RuntimeStepEvent(
        run_id="run-9",
        flow_name="poller",
        step_name="Build Summary",
        source_label="summary.xlsx",
        status="failed",
    )


def test_flow_log_entry_format_runtime_message_staticmethod_delegates_to_module_helper():
    message = "run=run-7 flow=poller source=C:/output/report.xlsx status=started"

    assert FlowLogEntry.format_runtime_message(message) == "poller  started  report.xlsx"

