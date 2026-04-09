"""Textual widget classes for the terminal UI surface."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label, ListItem, Static

from data_engine.domain import FlowRunState
from data_engine.views.flow_display import FlowRowDisplay, GroupRowDisplay
from data_engine.views.models import QtFlowCard
from data_engine.views.text import run_group_row_text


class FlowListItem(ListItem):
    """One flow entry in the TUI sidebar."""

    def __init__(self, card: QtFlowCard, state: str) -> None:
        self.card = card
        self.card_state = state
        self.label = Label()
        super().__init__(self.label)
        self.refresh_view(state)

    def refresh_view(self, state: str) -> None:
        self.card_state = state
        self.label.update(self._render_text(state))

    def _render_text(self, state: str) -> str:
        display = FlowRowDisplay.from_card(self.card, state, primary="title")
        return f"{display.dot} {display.primary}\n    {display.secondary}"


class GroupHeaderListItem(ListItem):
    """Non-selectable group header for configured flows."""

    def __init__(self, group_name: str, count: int) -> None:
        self.group_name = group_name
        flow_label = "flow" if count == 1 else "flows"
        display = GroupRowDisplay.from_group(group_name, [], {})
        self.label = Label(f"{display.uppercase_title}  {count} {flow_label}")
        super().__init__(self.label, disabled=True)


class RunGroupListItem(ListItem):
    """One grouped flow run entry in the logs pane."""

    def __init__(self, run_group: FlowRunState) -> None:
        self.run_group = run_group
        self.label = Label()
        super().__init__(self.label)
        self.refresh_view()

    def refresh_view(self, run_group: FlowRunState | None = None) -> None:
        if run_group is not None:
            self.run_group = run_group
        self.label.update(run_group_row_text(self.run_group))


class InfoModal(ModalScreen[None]):
    """Simple centered information modal for TUI drill-ins."""

    def __init__(self, *, title: str, body: str) -> None:
        super().__init__()
        self.title = title
        self.body = body

    CSS = """
    Screen {
        background: $background 70%;
    }

    #modal-shell {
        width: 1fr;
        height: 1fr;
        align: center middle;
    }

    #modal-card {
        width: 62;
        max-width: 72%;
        height: auto;
        max-height: 70%;
        border: round $surface;
        background: $panel;
        padding: 1 2;
    }

    #modal-title {
        height: auto;
        text-style: bold;
        padding-bottom: 1;
    }

    #modal-body {
        height: auto;
        max-height: 20;
    }
    """

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("enter", "dismiss", "Close"),
    ]

    def compose(self) -> ComposeResult:
        with Container(id="modal-shell"):
            with Vertical(id="modal-card"):
                yield Static(self.title, id="modal-title")
                yield Static(self.body, id="modal-body")
                yield Button("Close", id="close-modal")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close-modal":
            self.dismiss(None)

    def action_dismiss(self) -> None:
        self.dismiss(None)


__all__ = ["FlowListItem", "GroupHeaderListItem", "InfoModal", "RunGroupListItem"]
