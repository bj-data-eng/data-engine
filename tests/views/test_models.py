from __future__ import annotations

import logging

from data_engine.authoring.flow import Flow
from data_engine.domain import (
    ManualRunState,
    OperatorActionContext,
    PendingWorkspaceActionOverlay,
    RuntimeSessionState,
    SelectedFlowState,
    format_log_line,
    parse_runtime_event,
    short_source_label,
)
from data_engine.services.flow_catalog import flow_catalog_entry_from_flow
from data_engine.views.models import (
    default_flow_state,
    flow_catalog_entry_from_qt_card,
    flow_category,
    qt_flow_card_from_entry,
    qt_flow_cards_from_entries,
)
from data_engine.views.actions import GuiActionState
from data_engine.views.flow_display import FlowRowDisplay, GroupRowDisplay
from data_engine.views.presentation import (
    FlowGroupBucket,
    flow_secondary_text,
    format_seconds,
    group_cards,
    group_label,
    group_secondary_text,
    status_color_name,
)


def test_default_flow_state_matches_mode():
    assert default_flow_state("poll") == "poll ready"
    assert default_flow_state("schedule") == "schedule ready"
    assert default_flow_state("manual") == "manual"
    assert default_flow_state(None) == "manual"


def test_short_source_label_handles_empty_and_paths():
    assert short_source_label(None) == "-"
    assert short_source_label("") == "-"
    assert short_source_label("/tmp/example/report.xlsx") == "report.xlsx"


def test_format_log_line_renders_step_and_flow_records():
    step_record = logging.makeLogRecord(
        {"msg": "run=abc flow=docs_poll step=Write Parquet source=/tmp/input.xlsx status=success elapsed=0.2"}
    )
    flow_record = logging.makeLogRecord({"msg": "run=abc flow=docs_poll source=/tmp/input.xlsx status=started"})

    assert format_log_line(step_record) == "docs_poll  Write Parquet  success  input.xlsx"
    assert format_log_line(flow_record) == "docs_poll  started  input.xlsx"


def test_format_log_line_falls_back_to_filename_compaction():
    record = logging.makeLogRecord({"msg": "opened /tmp/example/input.xlsx and /tmp/example/output.parquet"})

    assert format_log_line(record) == "opened input.xlsx and output.parquet"


def test_parse_runtime_event_returns_none_for_unstructured_logs():
    record = logging.makeLogRecord({"msg": "plain log line"})
    assert parse_runtime_event(record) is None


def test_selected_flow_state_from_runtime_handles_missing_card():
    selected = SelectedFlowState.from_runtime(
        card=None,
        flow_states={},
        runtime_session=RuntimeSessionState.empty(),
        flow_groups_by_name={},
        active_flow_states={"running", "polling"},
    )

    assert selected.present is False
    assert selected.valid is False
    assert selected.running is False
    assert selected.group_active is False


def test_flow_category_distinguishes_automated_from_manual():
    assert flow_category("poll") == "automated"
    assert flow_category("schedule") == "automated"
    assert flow_category("manual") == "manual"


def test_flow_step_labels_are_reflected_in_operations():
    flow = (
        Flow(name="docs_poll", group="Docs")
        .step(lambda context: context.current, label="Read Excel")
        .step(lambda context: context.current, label="Write Parquet")
    )

    assert tuple(step.label for step in flow.steps) == ("Read Excel", "Write Parquet")


def test_flow_card_prefers_flow_label_over_internal_name():
    flow = Flow(name="docs_summary", label="Docs Summary", group="Docs")

    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(flow, description=None))

    assert card.name == "docs_summary"
    assert card.title == "Docs Summary"


def test_qt_flow_card_round_trips_catalog_entry_fields():
    entry = flow_catalog_entry_from_flow(
        Flow(name="docs_summary", label="Docs Summary", group="Docs"),
        description="Review claim output",
    )

    card = qt_flow_card_from_entry(entry)
    round_tripped = flow_catalog_entry_from_qt_card(card)

    assert round_tripped == entry


def test_flow_card_derives_readable_title_from_internal_name_when_label_missing():
    snake = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_summary", group="Docs"), description=None))
    camel = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="DocsSummary", group="Docs"), description=None))

    assert snake.title == "Docs Summary"
    assert camel.title == "Docs Summary"


def test_qt_flow_cards_from_entries_preserves_order_and_error_fields():
    first = flow_catalog_entry_from_flow(Flow(name="docs_summary", group="Docs"), description="Review")
    second = flow_catalog_entry_from_flow(Flow(name="broken_flow", group="Docs"), description="Broken")
    second = second.__class__(**{**second.__dict__, "valid": False, "error": "missing dependency"})

    cards = qt_flow_cards_from_entries((first, second))

    assert [card.name for card in cards] == ["docs_summary", "broken_flow"]
    assert cards[1].valid is False
    assert cards[1].error == "missing dependency"


def test_shared_flow_secondary_text_matches_mode_and_state():
    assert flow_secondary_text("poll", "poll ready") == "poll"
    assert flow_secondary_text("poll", "polling") == "polling"
    assert flow_secondary_text("poll", "stopping runtime") == "poll - stopping"
    assert flow_secondary_text("schedule", "schedule ready") == "schedule"
    assert flow_secondary_text("schedule", "scheduled") == "scheduled"
    assert flow_secondary_text("schedule", "stopping flow") == "schedule - stopping"
    assert flow_secondary_text("schedule", "failed") == "schedule - failed"
    assert flow_secondary_text("manual", "manual") == "manual"
    assert flow_secondary_text("manual", "stopping flow") == "manual - stopping"


def test_shared_group_secondary_text_summarizes_counts():
    cards = (
        qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_poll", group="Docs").watch(mode="poll", source="/tmp/in", interval="5s"), description=None)),
        qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_validate", group="Docs"), description=None)),
    )

    assert group_secondary_text(list(cards), {"docs_poll": "polling", "docs_validate": "manual"}) == "2 flow(s)  Running: 1"
    assert group_secondary_text(list(cards), {"docs_poll": "failed", "docs_validate": "manual"}) == "2 flow(s)  Error: 1"


def test_shared_status_color_and_duration_formatting_are_stable():
    assert status_color_name("stopping runtime") == "warning"
    assert format_seconds(0.0005) == "<1ms"
    assert format_seconds(1.239) == "1.2s"


def test_shared_action_state_builders_capture_surface_enablement_rules():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_summary", label="Docs Summary", group="Docs"), description=None))
    session = RuntimeSessionState.empty()
    selected = SelectedFlowState.from_runtime(
        card=card,
        flow_states={card.name: "manual"},
        runtime_session=session,
        flow_groups_by_name={card.name: card.group},
        active_flow_states={"running", "polling", "scheduled", "stopping flow", "stopping runtime"},
    )
    gui = GuiActionState.from_context(
        OperatorActionContext(
            runtime_session=session,
            selected_flow=selected,
            has_automated_flows=True,
        )
    )
    assert gui.flow_run_enabled is True
    assert gui.flow_run_state == "run"
    assert gui.engine_label == "Start Engine"


def test_shared_action_state_builders_cover_runtime_stopping_and_workspace_owned_branches():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_summary", label="Docs Summary", group="Docs"), description=None))
    session = RuntimeSessionState(
        workspace_owned=True,
        runtime_active=True,
        runtime_stopping=True,
        manual_runs=(ManualRunState(group_name="Docs", flow_name="docs_summary"),),
    )
    selected = SelectedFlowState(
        card=card,
        live_truth_known=True,
        live_state="stopping",
        live_manual_running=True,
        group_active=True,
    )
    context = OperatorActionContext(
        runtime_session=session,
        selected_flow=selected,
        has_automated_flows=True,
        workspace_available=False,
    )

    gui = GuiActionState.from_context(context)
    assert gui.flow_run_label == "Stopping..."
    assert gui.flow_run_enabled is False
    assert gui.engine_enabled is False
    assert gui.engine_label == "Stopping..."
    assert gui.engine_state == "running"
    assert gui.refresh_enabled is False
    assert gui.request_control_visible is True
    assert gui.request_control_enabled is False


def test_action_state_builders_allow_run_once_when_other_group_manual_run_is_active():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="manual_review", group="Manual"), description=None))
    selected = SelectedFlowState(card=card, state="manual", group_active=False)
    session = RuntimeSessionState.empty().with_manual_runs_map({"Imports": "docs2_parallel_poll"})
    context = OperatorActionContext(
        runtime_session=session,
        selected_flow=selected,
        has_automated_flows=True,
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)
    assert gui.flow_run_enabled is True
    assert gui.flow_run_state == "run"
    assert gui.engine_enabled is False


def test_gui_action_state_hides_request_control_when_takeover_is_already_available():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="manual_review", group="Manual"), description=None))
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState(workspace_owned=False, leased_by_machine_id=None),
        selected_flow=SelectedFlowState(card=card),
        has_automated_flows=True,
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)

    assert gui.request_control_visible is True
    assert gui.request_control_enabled is False


def test_action_state_builders_block_run_once_when_selected_group_is_active():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="manual_review", group="Manual"), description=None))
    selected = SelectedFlowState(card=card, live_truth_known=True, live_state="running", live_manual_running=True, group_active=True)
    session = RuntimeSessionState.empty().with_manual_runs_map({"Manual": "manual_review"})
    context = OperatorActionContext(
        runtime_session=session,
        selected_flow=selected,
        has_automated_flows=True,
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)
    assert gui.flow_run_label == "Stop Flow"
    assert gui.flow_run_state == "stop"
    assert gui.flow_run_enabled is True
    assert gui.engine_enabled is False


def test_action_state_builders_show_stop_flow_for_active_manual_run_while_engine_runs_elsewhere():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="manual_review", group="Manual"), description=None))
    selected = SelectedFlowState(card=card, live_truth_known=True, live_state="running", live_manual_running=True, group_active=True)
    session = RuntimeSessionState(runtime_active=True, active_runtime_flow_names=("poller",)).with_manual_runs_map({"Manual": "manual_review"})
    context = OperatorActionContext(
        runtime_session=session,
        selected_flow=selected,
        has_automated_flows=True,
        engine_state="running",
        engine_truth_known=True,
        live_truth_known=True,
        live_manual_run_active=True,
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)

    assert gui.flow_run_label == "Stop Flow"
    assert gui.flow_run_state == "stop"
    assert gui.flow_run_enabled is True
    assert gui.engine_label == "Stop Engine"
    assert gui.engine_enabled is True


def test_action_state_builders_do_not_show_stop_flow_for_engine_owned_selected_flow():
    card = qt_flow_card_from_entry(
        flow_catalog_entry_from_flow(
            Flow(name="poller", label="Poller", group="Docs").watch(
                mode="poll",
                source="/tmp/in",
                interval="5s",
            ),
            description=None,
        )
    )
    selected = SelectedFlowState(
        card=card,
        live_truth_known=True,
        live_state="running",
        live_manual_running=False,
        group_active=True,
    )
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState(runtime_active=True, active_runtime_flow_names=("poller",)),
        selected_flow=selected,
        has_automated_flows=True,
        engine_state="running",
        engine_truth_known=True,
        live_truth_known=True,
        live_manual_run_active=False,
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)

    assert gui.flow_run_label == "Running..."
    assert gui.flow_run_state == "run"
    assert gui.flow_run_enabled is False
    assert gui.engine_label == "Stop Engine"
    assert gui.engine_enabled is True


def test_action_state_builders_keep_stop_engine_enabled_while_manual_run_is_starting():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="manual_review", group="Manual"), description=None))
    selected = SelectedFlowState(card=card, live_truth_known=True, group_active=False)
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState(runtime_active=True, active_runtime_flow_names=("poller",)),
        selected_flow=selected,
        has_automated_flows=True,
        engine_state="running",
        engine_truth_known=True,
        live_truth_known=True,
        live_manual_run_active=False,
        workspace_available=True,
        overlay=PendingWorkspaceActionOverlay(
            pending_manual_run_groups=frozenset({"Manual"}),
        ),
    )

    gui = GuiActionState.from_context(context)

    assert gui.flow_run_label == "Starting..."
    assert gui.flow_run_enabled is False
    assert gui.engine_label == "Stop Engine"
    assert gui.engine_enabled is True


def test_action_state_builders_use_daemon_live_stopping_state_for_selected_flow():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="manual_review", group="Manual"), description=None))
    selected = SelectedFlowState(card=card, live_truth_known=True, live_state="stopping", group_active=True)
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState.empty(),
        selected_flow=selected,
        has_automated_flows=True,
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)
    assert gui.flow_run_label == "Stopping..."
    assert gui.flow_run_state == "stop"
    assert gui.flow_run_enabled is False


def test_action_state_builders_prefer_empty_daemon_live_truth_over_stale_manual_session():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="manual_review", group="Manual"), description=None))
    selected = SelectedFlowState(
        card=card,
        live_truth_known=True,
        live_manual_running=False,
        group_active=False,
    )
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState.empty().with_manual_runs_map({"Manual": "manual_review"}),
        selected_flow=selected,
        has_automated_flows=True,
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)

    assert gui.flow_run_label == "Run Once"
    assert gui.flow_run_state == "run"
    assert gui.flow_run_enabled is True


def test_action_state_builders_disable_runtime_controls_while_engine_is_starting():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_summary", group="Docs"), description=None))
    selected = SelectedFlowState(card=card)
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState.empty(),
        selected_flow=selected,
        has_automated_flows=True,
        engine_state="starting",
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)
    assert gui.engine_label == "Starting..."
    assert gui.engine_enabled is False
    assert gui.clear_flow_log_enabled is False


def test_action_state_builders_preserve_idle_daemon_engine_truth_over_stale_session_flags():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_summary", group="Docs"), description=None))
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState(runtime_active=True, runtime_stopping=True, active_runtime_flow_names=("poller",)),
        selected_flow=SelectedFlowState(card=card),
        has_automated_flows=True,
        engine_state="idle",
        engine_truth_known=True,
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)
    assert gui.engine_label == "Start Engine"
    assert gui.engine_enabled is True


def test_action_state_builders_fallback_to_session_when_no_live_truth_exists():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_summary", group="Docs"), description=None))
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState(runtime_active=True, runtime_stopping=True, active_runtime_flow_names=("poller",)).with_manual_runs_map({"Docs": "docs_summary"}),
        selected_flow=SelectedFlowState(card=card, state="running", group_active=True),
        has_automated_flows=True,
        engine_state="idle",
        engine_truth_known=False,
        live_truth_known=False,
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)
    assert gui.flow_run_label == "Stop Flow"
    assert gui.engine_label == "Stopping..."
    assert gui.engine_enabled is False


def test_action_state_builders_block_run_once_when_another_flow_in_same_group_is_active_live():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="manual_review", group="Manual"), description=None))
    selected = SelectedFlowState.from_runtime(
        card=card,
        flow_states={card.name: "manual"},
        runtime_session=RuntimeSessionState.empty(),
        flow_groups_by_name={card.name: card.group},
        active_flow_states={"running", "polling", "scheduled", "stopping flow", "stopping runtime"},
        live_runs={
            "run-other": type(
                "_Run",
                (),
                {
                    "flow_name": "other_manual_flow",
                    "group_name": "Manual",
                    "state": "running",
                },
            )(),
        },
    )
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState.empty(),
        selected_flow=selected,
        has_automated_flows=True,
        workspace_available=True,
        live_truth_known=True,
        live_manual_run_active=True,
    )

    gui = GuiActionState.from_context(context)
    assert selected.group_active is True
    assert gui.flow_run_label == "Run Once"
    assert gui.flow_run_enabled is False


def test_action_state_builders_disable_stop_engine_without_control():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_summary", group="Docs"), description=None))
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState(workspace_owned=False, leased_by_machine_id="remote", runtime_active=True),
        selected_flow=SelectedFlowState(card=card),
        has_automated_flows=True,
        engine_state="running",
        engine_truth_known=True,
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)
    assert gui.engine_label == "Stop Engine"
    assert gui.engine_enabled is False


def test_shared_action_state_builders_cover_control_unavailable_without_workspace_owner():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_summary", label="Docs Summary", group="Docs"), description=None))
    selected = SelectedFlowState(card=card, state="manual", group_active=False)
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState(workspace_owned=False, leased_by_machine_id="remote-host"),
        selected_flow=selected,
        has_automated_flows=True,
        workspace_available=True,
    )

    gui = GuiActionState.from_context(context)
    assert gui.flow_run_enabled is False
    assert gui.engine_enabled is False
    assert gui.clear_flow_log_enabled is False
    assert gui.request_control_enabled is True


def test_action_state_builders_disable_reset_and_request_control_while_request_is_pending():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_summary", group="Docs"), description=None))
    selected = SelectedFlowState(card=card, state="manual", group_active=False)
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState(workspace_owned=False, leased_by_machine_id="remote-host"),
        selected_flow=selected,
        has_automated_flows=True,
        workspace_available=True,
        local_request_pending=True,
    )

    gui = GuiActionState.from_context(context)

    assert gui.request_control_label == "Requesting..."
    assert gui.request_control_enabled is False
    assert gui.clear_flow_log_label == "Reset Flow"
    assert gui.clear_flow_log_enabled is False


def test_selected_flow_state_captures_group_activity_and_running_state():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(
        Flow(name="docs_summary", label="Docs Summary", group="Docs").watch(mode="poll", source="/tmp/in", interval="5s"),
        description=None,
    ))
    session = RuntimeSessionState.empty().with_manual_runs_map({"Docs": "docs_summary"})

    selected = SelectedFlowState.from_runtime(
        card=card,
        flow_states={card.name: "running"},
        runtime_session=session,
        flow_groups_by_name={card.name: card.group},
        active_flow_states={"running", "polling", "scheduled", "stopping flow", "stopping runtime"},
    )

    assert selected.present is True
    assert selected.valid is True
    assert selected.running is True
    assert selected.group_active is True


def test_shared_flow_display_builders_capture_row_metadata():
    card = qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_summary", label="Docs Summary", group="Docs"), description=None))

    flow_display = FlowRowDisplay.from_card(card, "manual")
    group_display = GroupRowDisplay.from_group("Docs", [card], {card.name: "manual"})

    assert flow_display.secondary == "manual"
    assert flow_display.state_color == "idle"
    assert group_display.title == "Docs"
    assert group_display.secondary == "1 flow(s)"


def test_shared_group_cards_orders_default_surface_buckets_first():
    cards = (
        qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="scheduled", group="schedule").watch(mode="schedule", time="10:30"), description=None)),
        qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="manual_one", group="manual"), description=None)),
        qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="poller", group="poll").watch(mode="poll", source="/tmp/in", interval="5s"), description=None)),
        qt_flow_card_from_entry(flow_catalog_entry_from_flow(Flow(name="docs_custom", group="Docs"), description=None)),
    )

    grouped = group_cards(cards)

    assert [bucket.group_name for bucket in grouped] == ["manual", "poll", "schedule", "Docs"]
    assert isinstance(grouped[0], FlowGroupBucket)
    assert group_label("manual") == "Manual"
    assert group_label("Docs") == "Docs"
