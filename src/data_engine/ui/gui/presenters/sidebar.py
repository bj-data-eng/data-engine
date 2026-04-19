"""Sidebar presentation helpers for the desktop UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtWidgets import QLabel, QFrame, QWidget

from data_engine.views.flow_display import FlowRowDisplay, GroupRowDisplay
from data_engine.views.presentation import flow_group_name

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def refresh_sidebar_selection(window: "DataEngineWindow") -> None:
    container = window.sidebar_content
    container.setUpdatesEnabled(False)
    try:
        for flow_name, widget in window.sidebar_flow_widgets.items():
            selected = flow_name == window.selected_flow_name
            flow_index = widget.property("flowIndex")
            for label in widget.findChildren(QLabel, "sidebarFlowNumber"):
                if isinstance(flow_index, int):
                    label.setText(f"{flow_index:02d}")
            if widget.property("selected") == selected:
                continue
            widget.setProperty("selected", selected)
            repolish_widget_tree(widget)
    finally:
        container.setUpdatesEnabled(True)
        container.update()


def refresh_sidebar_state_views(window: "DataEngineWindow", changed_flow_names: set[str]) -> bool:
    """Refresh sidebar labels/colors in place for state-only changes.

    Return True when the caller should fall back to a full sidebar rebuild.
    """
    if not changed_flow_names:
        return False
    if not window.sidebar_flow_widgets:
        return True

    container = window.sidebar_content
    container.setUpdatesEnabled(False)
    try:
        affected_groups: set[str] = set()
        for flow_name in changed_flow_names:
            card = window.flow_cards.get(flow_name)
            widget = window.sidebar_flow_widgets.get(flow_name)
            if card is None or widget is None:
                return True
            affected_groups.add(flow_group_name(card))
            state = window.flow_states.get(card.name, card.state)
            flow_display = FlowRowDisplay.from_card(card, state, primary="name")
            widget.setToolTip("")
            for label in widget.findChildren(QLabel, "sidebarFlowMeta"):
                label.setText(flow_display.secondary)
                label.setProperty("stateColor", flow_display.state_color)
                repolish_widget_tree(label)
            for label in widget.findChildren(QLabel, "sidebarStateDot"):
                label.setProperty("stateColor", flow_display.state_color)
                repolish_widget_tree(label)

        for group_name in affected_groups:
            group_widget = window.sidebar_group_widgets.get(group_name)
            if group_widget is None:
                return True
            entries = [card for card in window.flow_cards.values() if flow_group_name(card) == group_name]
            group_display = GroupRowDisplay.from_group(group_name, entries, window.flow_states)
            for label in group_widget.findChildren(QLabel, "sidebarGroupMeta"):
                label.setText(group_display.secondary)
    finally:
        container.setUpdatesEnabled(True)
        container.update()

    return False


def set_hovered(widget: QFrame, hovered: bool) -> None:
    """Update one sidebar row hover property and repolish it."""
    if widget.property("hovered") == hovered:
        return
    widget.setProperty("hovered", hovered)
    repolish_widget_tree(widget)


def repolish_widget_tree(widget: QWidget) -> None:
    """Reapply stylesheet state to one widget and its child widgets."""
    style = widget.style()
    style.unpolish(widget)
    style.polish(widget)
    for child in widget.findChildren(QWidget):
        child.update()
    widget.update()


__all__ = ["refresh_sidebar_selection", "refresh_sidebar_state_views", "repolish_widget_tree", "set_hovered"]
