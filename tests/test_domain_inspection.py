from __future__ import annotations

from data_engine.authoring.flow import Flow
from data_engine.domain import ConfigPreviewState, StepOutputIndex
from data_engine.services.flow_catalog import flow_catalog_entry_from_flow
from data_engine.views.models import qt_flow_card_from_entry


def _sample_card():
    return qt_flow_card_from_entry(
        flow_catalog_entry_from_flow(
            Flow(name="claims_summary", label="Claims Summary", group="Claims"),
            description="Review claims",
        )
    )


def _sample_parallel_card():
    return qt_flow_card_from_entry(
        flow_catalog_entry_from_flow(
            Flow(name="claims_poll", label="Claims Poll", group="Claims").watch(
                mode="poll",
                source="/tmp/incoming",
                interval="5s",
                max_parallel=4,
            ),
            description="Poll claims",
        )
    )


def test_config_preview_state_keeps_title_description_and_summary_rows():
    preview = ConfigPreviewState.from_flow(_sample_card(), {"claims_summary": "running"})

    assert preview.title == "Claims Summary"
    assert preview.description == "Review claims"
    assert preview.summary.rows[0].label == "Flow"
    assert preview.summary.rows[0].value == "claims_summary"
    assert ("Max Parallel", "1") in tuple((row.label, row.value) for row in preview.summary.rows)


def test_step_output_index_wraps_flow_operation_lookup():
    index = StepOutputIndex.from_mapping({"claims": {"Write Output": __import__("pathlib").Path("/tmp/output.parquet")}})

    assert index.has_output("claims", "Write Output") is True
    assert index.output_path("claims", "Write Output").name == "output.parquet"


def test_config_preview_state_exposes_configured_parallelism():
    preview = ConfigPreviewState.from_flow(_sample_parallel_card(), {"claims_poll": "poll ready"})

    assert ("Max Parallel", "4") in tuple((row.label, row.value) for row in preview.summary.rows)
