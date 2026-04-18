from __future__ import annotations

import pytest
from textual.widgets import ListView

from data_engine.domain import FlowLogEntry, RuntimeStepEvent
from data_engine.ui.tui.app import RunGroupListItem

from tests.tui.support import FakeLogService, RecordingTui, make_tui


@pytest.mark.anyio
async def test_tui_log_run_selection_updates_preview():
    app = make_tui(log_service=FakeLogService(), app_cls=RecordingTui)
    async with app.run_test():
        flow_name = app.flow_cards[0].name
        app.selected_flow_name = flow_name
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-a started",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="file_a.xlsx",
                    status="started",
                    elapsed_seconds=0.004,
                ),
            )
        )
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-a read",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-a",
                    flow_name=flow_name,
                    step_name="Read",
                    source_label="file_a.xlsx",
                    status="success",
                    elapsed_seconds=0.004,
                ),
            )
        )
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-b started",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-b",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="file_b.xlsx",
                    status="started",
                    elapsed_seconds=0.009,
                ),
            )
        )
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-b read",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-b",
                    flow_name=flow_name,
                    step_name="Read",
                    source_label="file_b.xlsx",
                    status="success",
                    elapsed_seconds=0.009,
                ),
            )
        )

        app._render_selected_flow()

        run_groups = app.log_store.runs_for_flow(flow_name)
        assert len(run_groups) == 2

        app.selected_run_key = next(run_group.key for run_group in run_groups if run_group.key[1] == "run-a")
        app.action_view_log()

        assert app.shown_screens
        assert "Run Details" in app.shown_screens[0][0]
        assert "file_a.xlsx" in app.shown_screens[0][0] or "file_a.xlsx" in app.shown_screens[0][1]


@pytest.mark.anyio
async def test_tui_selecting_log_run_opens_modal():
    app = make_tui(log_service=FakeLogService(), app_cls=RecordingTui)
    async with app.run_test():
        flow_name = app.flow_cards[0].name
        app.selected_flow_name = flow_name
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-a started",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="file_a.xlsx",
                    status="started",
                    elapsed_seconds=0.004,
                ),
            )
        )
        app._render_selected_flow()

        run_group = app.log_store.runs_for_flow(flow_name)[0]
        app.on_list_view_selected(type("Evt", (), {"item": RunGroupListItem(run_group)})())

        assert app.shown_screens
        assert "Run Details" in app.shown_screens[0][0]


@pytest.mark.anyio
async def test_tui_run_group_row_refreshes_when_same_run_finishes(monkeypatch):
    app = make_tui(log_service=FakeLogService())
    async with app.run_test():
        flow_name = app.flow_cards[0].name
        app.selected_flow_name = flow_name
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-a started",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="file_a.xlsx",
                    status="started",
                ),
            )
        )

        app._render_selected_flow()
        run_list = app.query_one("#log-run-list", ListView)
        run_item = next(child for child in run_list.children if isinstance(child, RunGroupListItem))
        assert run_item.run_group.status == "started"

        app.log_store.append_entry(
            FlowLogEntry(
                line="run-a success",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="file_a.xlsx",
                    status="success",
                    elapsed_seconds=0.023,
                ),
            )
        )

        app._render_selected_flow()

        run_item = next(child for child in run_list.children if isinstance(child, RunGroupListItem))
        assert run_item.run_group.status == "success"

