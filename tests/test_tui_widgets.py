from __future__ import annotations

from types import SimpleNamespace

import pytest

textual = pytest.importorskip("textual")

from data_engine.domain import FlowLogEntry, FlowRunState, RunStepState, RuntimeStepEvent
from data_engine.ui.tui.widgets import FlowListItem, GroupHeaderListItem, InfoModal, RunGroupListItem
from data_engine.views.models import QtFlowCard


def _card(*, mode: str = "poll", state: str = "polling", valid: bool = True) -> QtFlowCard:
    return QtFlowCard(
        name="claims_poller",
        group="Claims",
        title="Claims Poller",
        description="Polls for new claim workbooks.",
        source_root="/tmp/input",
        target_root="/tmp/output",
        mode=mode,
        interval="5s" if mode != "manual" else "-",
        operations="Read -> Write",
        operation_items=("Read", "Write"),
        state=state,
        valid=valid,
        category="automated" if mode != "manual" else "manual",
        error="",
    )


def _run_group(*, status: str = "success", source_label: str = "input.xlsx", elapsed_seconds: float = 1.2) -> FlowRunState:
    summary = FlowLogEntry(
        line="started",
        kind="flow",
        event=RuntimeStepEvent(
            run_id="run-1",
            flow_name="claims_poller",
            step_name="Read",
            source_label=source_label,
            status=status,
            elapsed_seconds=elapsed_seconds,
        ),
    )
    return FlowRunState(
        key=("claims_poller", "run-1"),
        display_label="2026-04-04 09:15:00 AM",
        source_label=source_label,
        status=status,
        elapsed_seconds=elapsed_seconds,
        summary_entry=summary,
        steps=(
            RunStepState(
                step_name="Read",
                status=status,
                elapsed_seconds=elapsed_seconds,
                entry=summary,
            ),
        ),
        entries=(summary,),
    )


def test_flow_list_item_renders_and_refreshes_current_state():
    item = FlowListItem(_card(), "polling")

    assert item.card.name == "claims_poller"
    assert item.card_state == "polling"
    assert "Claims Poller" in item.label.render().plain
    assert "*" in item.label.render().plain

    item.refresh_view("success")

    rendered = item.label.render().plain
    assert "success" in rendered
    assert item.card_state == "success"


def test_group_header_list_item_uses_uppercase_title_and_pluralization():
    singular = GroupHeaderListItem("Claims", 1)
    plural = GroupHeaderListItem("Claims", 2)

    assert singular.group_name == "Claims"
    assert singular.disabled is True
    assert singular.label.render().plain == "CLAIMS  1 flow"
    assert plural.label.render().plain == "CLAIMS  2 flows"


def test_run_group_list_item_refreshes_from_current_run_group():
    item = RunGroupListItem(_run_group())

    assert "SUCCESS" in item.label.render().plain
    assert "input.xlsx" in item.label.render().plain

    updated = _run_group(status="failed", source_label="output.xlsx", elapsed_seconds=2.5)
    item.refresh_view(updated)

    assert item.run_group == updated
    rendered = item.label.render().plain
    assert "FAILED" in rendered
    assert "output.xlsx" in rendered


@pytest.mark.anyio
async def test_info_modal_compose_and_dismiss_bindings_are_wired():
    class RecordingInfoModal(InfoModal):
        def __init__(self, *, title: str, body: str) -> None:
            super().__init__(title=title, body=body)
            self.dismissed: list[object] = []

        def dismiss(self, result: object = None) -> None:  # type: ignore[override]
            self.dismissed.append(result)

    modal = RecordingInfoModal(title="Workspace Error", body="The daemon is not available.")

    from textual.app import App
    from textual.widgets import Button, Static

    class ModalApp(App[None]):
        def __init__(self, screen: RecordingInfoModal) -> None:
            super().__init__()
            self.screen_to_show = screen

        async def on_mount(self) -> None:
            await self.push_screen(self.screen_to_show)

    async with ModalApp(modal).run_test() as pilot:
        await pilot.pause()
        shell = pilot.app.screen.query_one("#modal-shell")
        title = pilot.app.screen.query_one("#modal-title", Static)
        body = pilot.app.screen.query_one("#modal-body", Static)
        button = pilot.app.screen.query_one("#close-modal", Button)

        assert modal.title == "Workspace Error"
        assert modal.body == "The daemon is not available."
        assert [binding.key for binding in modal.BINDINGS] == ["escape", "enter"]
        assert shell.id == "modal-shell"
        assert title.render().plain == "Workspace Error"
        assert body.render().plain == "The daemon is not available."
        assert button.label == "Close"

        modal.action_dismiss()
        modal.on_button_pressed(SimpleNamespace(button=SimpleNamespace(id="close-modal")))
        modal.on_button_pressed(SimpleNamespace(button=SimpleNamespace(id="other")))

        assert modal.dismissed == [None, None]
