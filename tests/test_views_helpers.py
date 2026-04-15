from __future__ import annotations

from pathlib import Path

from data_engine.authoring.flow import Flow
from data_engine.domain import FlowLogEntry, FlowRunState, OperatorActionContext, RunStepState, RuntimeSessionState, RuntimeStepEvent, SelectedFlowState
from data_engine.services.flow_catalog import flow_catalog_entry_from_flow
from data_engine.views.actions import GuiActionState, TuiActionState
from data_engine.views.artifacts import classify_artifact_preview, is_text_artifact
from data_engine.views.flow_display import FlowRowDisplay, GroupRowDisplay
from data_engine.views.models import qt_flow_card_from_entry
from data_engine.views.presentation import (
    FlowGroupBucket,
    flow_group_name,
    flow_secondary_text,
    format_seconds,
    operation_marker,
    state_dot,
    status_color_name,
)
from data_engine.views.runs import RunGroupDisplay, format_raw_log_message
from data_engine.views.status import WORKSPACE_UNAVAILABLE_TEXT, surface_control_status_text
from data_engine.views.text import format_optional_seconds, pad, render_run_group_lines, run_group_row_text, short_datetime


def _card(name: str, *, group: str | None = "Claims", mode: str = "manual", valid: bool = True):
    flow = Flow(name=name, group=group)
    if mode == "poll":
        flow = flow.watch(mode="poll", source="/tmp/input", interval="5s")
    elif mode == "schedule":
        flow = flow.watch(mode="schedule", time="10:30")
    entry = flow_catalog_entry_from_flow(flow, description="Example flow")
    card = qt_flow_card_from_entry(entry)
    if valid:
        return card
    return card.__class__(**{**card.__dict__, "valid": False})


def test_flow_group_name_falls_back_to_mode_when_group_missing():
    card = _card("poller", mode="poll").__class__(
        **{**_card("poller", mode="poll").__dict__, "group": None}
    )

    assert flow_group_name(card) == "poll"


def test_flow_row_display_uses_failed_dot_for_invalid_cards():
    card = _card("broken_flow", valid=False)

    display = FlowRowDisplay.from_card(card, "manual", primary="name")

    assert display.primary == "broken_flow"
    assert display.dot == "!"
    assert display.tooltip.endswith("| group=Claims")


def test_flow_row_display_keeps_state_color_from_requested_state_for_invalid_card():
    card = _card("broken_flow", valid=False)

    display = FlowRowDisplay.from_card(card, "running", primary="title")

    assert display.primary == "Broken Flow"
    assert display.state_color == "success"
    assert display.dot == "!"


def test_flow_row_display_keeps_unknown_group_out_of_tooltip_and_uses_name_primary():
    card = _card("manual_review").__class__(**{**_card("manual_review").__dict__, "group": None})

    display = FlowRowDisplay.from_card(card, "manual", primary="name")

    assert display.primary == "manual_review"
    assert display.secondary == "Manual"
    assert display.state_color == "idle"
    assert display.dot == "·"
    assert "group=" not in display.tooltip


def test_presentation_helpers_cover_remaining_state_branches():
    assert flow_secondary_text("poll", "failed") == "Polling  failed"
    assert flow_secondary_text("manual", "failed") == "Manual  failed"
    assert status_color_name("started") == "started"
    assert status_color_name("manual") == "idle"
    assert state_dot("stopping flow") == "~"
    assert state_dot("manual") == "·"
    assert operation_marker("running") == ">"
    assert operation_marker("idle") == "·"
    assert format_seconds(0.048) == "48ms"
    assert format_seconds(0.04899) == "48ms"
    assert format_seconds(0.9999) == "999ms"
    assert format_seconds(61.2) == "1.0m"
    assert format_seconds(3665.9) == "1.0h"


def test_text_helpers_cover_padding_datetime_and_optional_duration_edges():
    assert pad("abcdef", 1) == "a"
    assert pad("abcdef", 4) == "abc…"
    assert short_datetime("2026-04-06") == "2026-04-06"
    assert short_datetime("2026-04-06 09:15:00 AM") == "09:15:00 AM"
    assert short_datetime("09:15:00 AM") == "09:15:00 AM"
    assert format_optional_seconds(None) == "-"
    assert format_optional_seconds(1.23) == "1.2s"


def test_run_group_row_text_uses_default_duration_placeholder():
    run_state = FlowRunState(
        key=("claims_summary", "run-1"),
        display_label="2026-04-06 09:15:00 AM",
        source_label="input.xlsx",
        status="running",
        elapsed_seconds=None,
        summary_entry=None,
        steps=(),
        entries=(),
    )

    row = run_group_row_text(run_state)

    assert "RUNNING" in row
    assert "input.xlsx" in row
    assert " -" in row


def test_render_run_group_lines_keeps_placeholder_duration_for_missing_step_elapsed():
    step_entry = FlowLogEntry(
        line="step",
        kind="flow",
        flow_name="claims_summary",
        event=RuntimeStepEvent(
            run_id="run-1",
            flow_name="claims_summary",
            step_name="Read Excel",
            source_label="input.xlsx",
            status="failed",
            elapsed_seconds=None,
        ),
    )
    run_state = FlowRunState(
        key=("claims_summary", "run-1"),
        display_label="2026-04-06 09:15:00 AM",
        source_label="input.xlsx",
        status="failed",
        elapsed_seconds=None,
        summary_entry=None,
        steps=(
            RunStepState(
                step_name="Read Excel",
                status="failed",
                elapsed_seconds=None,
                entry=step_entry,
            ),
        ),
        entries=(),
    )

    lines = render_run_group_lines(run_state)

    assert "[FAILED]" in lines[0]
    assert lines[1].endswith(" -")


def test_run_group_display_uses_gui_canonical_status_mapping():
    run_state = FlowRunState(
        key=("claims_summary", "run-1"),
        display_label="2026-04-06 09:15:00 AM",
        source_label="input.xlsx",
        status="stopped",
        elapsed_seconds=1.2,
        summary_entry=None,
        steps=(),
        entries=(),
    )

    display = RunGroupDisplay.from_run(run_state)

    assert display.primary_label == "2026-04-06 09:15:00 AM"
    assert display.status_text == "Stopped"
    assert display.status_visual_state == "failed"
    assert display.duration_text == "1.2s"


def test_run_group_display_maps_started_runs_to_started_visual_state_without_duration():
    run_state = FlowRunState(
        key=("claims_summary", "run-2"),
        display_label="2026-04-06 09:16:00 AM",
        source_label="-",
        status="started",
        elapsed_seconds=None,
        summary_entry=None,
        steps=(),
        entries=(),
    )

    display = RunGroupDisplay.from_run(run_state)

    assert display.status_text == "Started"
    assert display.status_visual_state == "started"
    assert display.duration_text is None


def test_run_group_display_maps_success_runs_to_finished_visual_state():
    run_state = FlowRunState(
        key=("claims_summary", "run-3"),
        display_label="2026-04-06 09:17:00 AM",
        source_label="input.xlsx",
        status="success",
        elapsed_seconds=0.25,
        summary_entry=None,
        steps=(),
        entries=(),
    )

    display = RunGroupDisplay.from_run(run_state)

    assert display.status_text == "Success"
    assert display.status_visual_state == "finished"
    assert display.duration_text == "250ms"


def test_format_raw_log_message_omits_placeholder_source_separator():
    entry = FlowLogEntry(
        line="run=abc flow=claims_summary step=Collect Claim Files source=None status=started",
        kind="runtime",
        flow_name="claims_summary",
        event=RuntimeStepEvent(
            run_id="abc",
            flow_name="claims_summary",
            step_name="Collect Claim Files",
            source_label="-",
            status="started",
        ),
    )

    rendered = format_raw_log_message(entry)

    assert "claims_summary &gt; &gt;" not in rendered
    assert rendered == "claims_summary &gt; <b>Collect Claim Files</b> - <i>started</i>"


def test_format_raw_log_message_handles_flow_level_event_without_source():
    entry = FlowLogEntry(
        line="run=abc flow=claims_summary source=None status=success",
        kind="runtime",
        flow_name="claims_summary",
        event=RuntimeStepEvent(
            run_id="abc",
            flow_name="claims_summary",
            step_name=None,
            source_label="-",
            status="success",
        ),
    )

    assert format_raw_log_message(entry) == "claims_summary &gt; <i>success</i>"


def test_format_raw_log_message_handles_flow_level_event_with_escaped_source():
    entry = FlowLogEntry(
        line="run=abc flow=claims_summary source=input<1>.xlsx status=success",
        kind="runtime",
        flow_name="claims_summary",
        event=RuntimeStepEvent(
            run_id="abc",
            flow_name="claims_summary",
            step_name=None,
            source_label="input<1>.xlsx",
            status="success",
        ),
    )

    assert format_raw_log_message(entry) == "claims_summary &gt; input&lt;1&gt;.xlsx &gt; <i>success</i>"


def test_format_raw_log_message_escapes_html_in_unstructured_lines_and_step_names():
    plain = FlowLogEntry(line="<b>alert</b>", kind="system", flow_name=None)
    structured = FlowLogEntry(
        line="ignored",
        kind="runtime",
        flow_name="claims_summary",
        event=RuntimeStepEvent(
            run_id="abc",
            flow_name="claims_summary",
            step_name="Collect <Claims>",
            source_label="input<1>.xlsx",
            status="started",
        ),
    )

    assert format_raw_log_message(plain) == "&lt;b&gt;alert&lt;/b&gt;"
    assert format_raw_log_message(structured) == (
        "claims_summary &gt; input&lt;1&gt;.xlsx &gt; <b>Collect &lt;Claims&gt;</b> - <i>started</i>"
    )


def test_surface_status_helpers_cover_workspace_and_empty_flow_fallback():
    assert WORKSPACE_UNAVAILABLE_TEXT == "Workspace root is no longer available."
    assert surface_control_status_text("This Workstation has control") == "This Workstation has control"
    assert surface_control_status_text(None, empty_flow_message="No discoverable flows were found yet.") == "No discoverable flows were found yet."
    assert surface_control_status_text(None) == ""
    assert surface_control_status_text("", empty_flow_message="No discoverable flows were found yet.") == "No discoverable flows were found yet."


def test_artifact_preview_classification_keeps_gui_canonical_labels_and_messages(tmp_path):
    assert classify_artifact_preview(tmp_path / "output.parquet").label == "Parquet table preview"
    assert classify_artifact_preview(tmp_path / "workbook.xlsx").label == "Excel table preview"
    assert classify_artifact_preview(tmp_path / "notes.txt").kind == "text"
    assert classify_artifact_preview(tmp_path / "packet.pdf").placeholder_message == (
        "PDF artifacts are recognized, but in-app PDF text inspection is not available yet."
    )
    assert classify_artifact_preview(tmp_path / "blob.bin").placeholder_message == (
        "This artifact type is not previewable in the UI yet."
    )


def test_artifact_preview_text_detection_covers_mimetype_fallback_and_unknown_suffix():
    assert is_text_artifact(Path("styles.css")) is True
    assert is_text_artifact(Path("archive.weirdbin")) is False


def test_action_state_builders_cover_control_and_runtime_branches():
    card = _card("claims_summary")
    selected = SelectedFlowState(card=card, state="running", group_active=True, has_logs=True)
    session = RuntimeSessionState(runtime_active=True)
    context = OperatorActionContext(
        runtime_session=session,
        selected_flow=selected,
        has_automated_flows=True,
        workspace_available=False,
        selected_run_group_present=True,
    )

    gui = GuiActionState.from_context(context)
    tui = TuiActionState.from_context(context)

    assert gui.flow_run_label == "Running..."
    assert gui.flow_run_state == "run"
    assert gui.flow_run_enabled is False
    assert gui.engine_label == "Stop Engine"
    assert gui.refresh_enabled is False
    assert gui.clear_flow_log_enabled is True
    assert tui.refresh_disabled is True
    assert tui.run_once_disabled is True
    assert tui.start_engine_disabled is True
    assert tui.stop_engine_disabled is False
    assert tui.view_log_disabled is False


def test_action_state_builders_cover_idle_control_available_branches():
    card = _card("claims_summary")
    selected = SelectedFlowState(card=card, state="manual", group_active=False, has_logs=False)
    session = RuntimeSessionState(workspace_owned=False, leased_by_machine_id=None, runtime_active=False, runtime_stopping=False)
    context = OperatorActionContext(
        runtime_session=session,
        selected_flow=selected,
        has_automated_flows=False,
        workspace_available=True,
        selected_run_group_present=False,
    )

    gui = GuiActionState.from_context(context)
    tui = TuiActionState.from_context(context)

    assert gui.flow_run_enabled is True
    assert gui.flow_run_state == "run"
    assert gui.engine_enabled is False
    assert gui.engine_label == "Start Engine"
    assert gui.request_control_enabled is True
    assert tui.refresh_disabled is False
    assert tui.run_once_disabled is False
    assert tui.start_engine_disabled is False
    assert tui.stop_engine_disabled is True
    assert tui.view_config_disabled is False
    assert tui.clear_flow_log_disabled is False


def test_action_state_builders_cover_no_selection_idle_branches():
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState.empty(),
        selected_flow=SelectedFlowState(card=None),
        has_automated_flows=False,
        workspace_available=True,
        selected_run_group_present=False,
    )

    gui = GuiActionState.from_context(context)
    tui = TuiActionState.from_context(context)

    assert gui.flow_run_label == "Run Once"
    assert gui.flow_run_state == "run"
    assert gui.flow_run_enabled is False
    assert gui.flow_config_enabled is False
    assert gui.clear_flow_log_enabled is False
    assert gui.request_control_enabled is False
    assert tui.view_config_disabled is True
    assert tui.view_log_disabled is True
    assert tui.clear_flow_log_disabled is True


def test_group_row_display_from_bucket_uses_shared_group_title_and_error_summary():
    first = _card("claims_poll", group="Claims", mode="poll")
    second = _card("claims_manual", group="Claims")
    display = GroupRowDisplay.from_bucket(
        FlowGroupBucket(
            group_name="Claims",
            entries=(first, second),
        ),
        {"claims_poll": "failed", "claims_manual": "manual"},
    )

    assert display.title == "Claims"
    assert display.uppercase_title == "CLAIMS"
    assert display.secondary == "2 flow(s)  Error: 1"
