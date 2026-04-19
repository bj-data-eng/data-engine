from __future__ import annotations

from data_engine.authoring.flow import Flow
from data_engine.domain import OperatorActionContext, RuntimeSessionState, SelectedFlowState
from data_engine.services.flow_catalog import flow_catalog_entry_from_flow
from data_engine.views.actions import GuiActionState, TuiActionState
from data_engine.views.models import qt_flow_card_from_entry


def test_selected_flow_state_reflects_running_and_group_activity():
    card = qt_flow_card_from_entry(
        flow_catalog_entry_from_flow(
            Flow(name="claims_summary", label="Claims Summary", group="Claims").watch(
                mode="poll",
                source="/tmp/in",
                interval="5s",
            ),
            description=None,
        )
    )
    session = RuntimeSessionState.empty().with_manual_runs_map({"Claims": "claims_summary"})

    selected = SelectedFlowState.from_runtime(
        card=card,
        flow_states={card.name: "running"},
        runtime_session=session,
        flow_groups_by_name={card.name: card.group},
        active_flow_states={"running", "polling", "scheduled", "stopping flow", "stopping runtime"},
        has_logs=True,
    )

    assert selected.present is True
    assert selected.valid is True
    assert selected.running is True
    assert selected.group_active is True
    assert selected.has_logs is True


def test_action_context_builders_resolve_gui_and_tui_states():
    card = qt_flow_card_from_entry(
        flow_catalog_entry_from_flow(
            Flow(name="claims_summary", label="Claims Summary", group="Claims"),
            description=None,
        )
    )
    session = RuntimeSessionState.empty()
    selected = SelectedFlowState(card=card, has_logs=True)
    context = OperatorActionContext(
        runtime_session=session,
        selected_flow=selected,
        has_automated_flows=True,
        selected_run_group_present=False,
    )

    gui = GuiActionState.from_context(context)
    tui = TuiActionState.from_context(context)

    assert gui.flow_run_enabled is True
    assert gui.engine_label == "Start Engine"
    assert tui.run_once_disabled is False
    assert tui.view_log_disabled is True


def test_action_states_disable_runtime_controls_when_workspace_is_missing():
    card = qt_flow_card_from_entry(
        flow_catalog_entry_from_flow(
            Flow(name="claims_summary", label="Claims Summary", group="Claims"),
            description=None,
        )
    )
    session = RuntimeSessionState.empty()
    selected = SelectedFlowState(card=card, has_logs=True)
    context = OperatorActionContext(
        runtime_session=session,
        selected_flow=selected,
        has_automated_flows=True,
        workspace_available=False,
        selected_run_group_present=False,
    )

    gui = GuiActionState.from_context(context)
    tui = TuiActionState.from_context(context)

    assert gui.flow_run_enabled is False
    assert gui.engine_enabled is False
    assert tui.run_once_disabled is True
    assert tui.start_engine_disabled is True


def test_action_states_allow_manual_run_while_engine_is_active_for_another_group():
    card = qt_flow_card_from_entry(
        flow_catalog_entry_from_flow(
            Flow(name="manual_claims", label="Manual Claims", group="Manual"),
            description=None,
        )
    )
    session = RuntimeSessionState(runtime_active=True, active_runtime_flow_names=("poller",))
    selected = SelectedFlowState(card=card, state="manual", group_active=False, has_logs=False)
    context = OperatorActionContext(
        runtime_session=session,
        selected_flow=selected,
        has_automated_flows=True,
        workspace_available=True,
        selected_run_group_present=False,
    )

    gui = GuiActionState.from_context(context)
    tui = TuiActionState.from_context(context)

    assert gui.flow_run_enabled is True
    assert tui.run_once_disabled is False


def test_action_states_disable_run_once_for_automated_flow_while_engine_is_starting():
    card = qt_flow_card_from_entry(
        flow_catalog_entry_from_flow(
            Flow(name="poller", label="Poller", group="Claims").watch(
                mode="poll",
                source="/tmp/in",
                interval="5s",
            ),
            description=None,
        )
    )
    context = OperatorActionContext(
        runtime_session=RuntimeSessionState.empty(),
        selected_flow=SelectedFlowState(card=card, has_logs=False),
        has_automated_flows=True,
        engine_state="starting",
        workspace_available=True,
        selected_run_group_present=False,
    )

    gui = GuiActionState.from_context(context)
    tui = TuiActionState.from_context(context)

    assert gui.flow_run_enabled is False
    assert tui.run_once_disabled is True


def test_selected_flow_state_does_not_treat_engine_owned_flow_as_manual_running():
    card = qt_flow_card_from_entry(
        flow_catalog_entry_from_flow(
            Flow(name="claims_summary", label="Claims Summary", group="Claims").watch(
                mode="poll",
                source="/tmp/in",
                interval="5s",
            ),
            description=None,
        )
    )

    selected = SelectedFlowState.from_runtime(
        card=card,
        flow_states={card.name: "polling"},
        runtime_session=RuntimeSessionState(runtime_active=True, active_runtime_flow_names=("claims_summary",)),
        flow_groups_by_name={card.name: card.group},
        active_flow_states={"running", "polling", "scheduled", "stopping flow", "stopping runtime"},
        has_logs=True,
        live_runs={
            "run-1": type(
                "Run",
                (),
                {
                    "flow_name": "claims_summary",
                    "group_name": "Claims",
                    "state": "running",
                },
            )(),
        },
        engine_active_flow_names=("claims_summary",),
    )

    assert selected.live_state == "running"
    assert selected.live_manual_running is False
    assert selected.group_active is True
