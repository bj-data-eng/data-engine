from __future__ import annotations

from data_engine.authoring.flow import Flow
from data_engine.domain import ConfigPreviewState, StepOutputIndex
from data_engine.services.flow_catalog import flow_catalog_entry_from_flow
from data_engine.views.models import qt_flow_card_from_entry


def _sample_card():
    return qt_flow_card_from_entry(
        flow_catalog_entry_from_flow(
            Flow(name="docs_summary", label="Docs Summary", group="Docs"),
            description="Review docs",
        )
    )


def _sample_parallel_card():
    return qt_flow_card_from_entry(
        flow_catalog_entry_from_flow(
            Flow(name="docs_poll", label="Docs Poll", group="Docs").watch(
                mode="poll",
                source="/tmp/incoming",
                interval="5s",
                settle=3,
                max_parallel=4,
            ),
            description="Poll docs",
        )
    )


def test_config_preview_state_keeps_title_description_and_summary_rows():
    preview = ConfigPreviewState.from_flow(_sample_card(), {"docs_summary": "running"})

    assert preview.title == "Docs Summary"
    assert preview.description == "Review docs"
    assert preview.summary.rows[0].label == "Flow"
    assert preview.summary.rows[0].value == "docs_summary"
    assert ("Max Parallel", "1") in tuple((row.label, row.value) for row in preview.summary.rows)


def test_step_output_index_wraps_flow_operation_lookup():
    index = StepOutputIndex.from_mapping({"docs": {"Write Output": __import__("pathlib").Path("/tmp/output.parquet")}})

    assert index.has_output("docs", "Write Output") is True
    assert index.output_path("docs", "Write Output").name == "output.parquet"


def test_config_preview_state_exposes_configured_parallelism():
    preview = ConfigPreviewState.from_flow(_sample_parallel_card(), {"docs_poll": "poll ready"})

    assert ("Max Parallel", "4") in tuple((row.label, row.value) for row in preview.summary.rows)
    assert ("Settle", "3") in tuple((row.label, row.value) for row in preview.summary.rows)

