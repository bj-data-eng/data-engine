from __future__ import annotations

from data_engine.ui.tui.app import FlowListItem
from data_engine.views.models import QtFlowCard


def test_flow_list_item_refresh_view_updates_label():
    card = QtFlowCard(
        name="claims_summary",
        group="Claims",
        title="Claims Summary",
        description="",
        source_root="/tmp/in",
        target_root="/tmp/out",
        mode="schedule",
        interval="5s",
        settle="-",
        operations="Read -> Write",
        operation_items=("Read", "Write"),
        state="schedule ready",
        valid=True,
        category="automated",
    )

    item = FlowListItem(card, "schedule ready")
    item.refresh_view("success")

    rendered = item.label.render().plain
    assert "Claims Summary" in rendered
    assert "success" in rendered

