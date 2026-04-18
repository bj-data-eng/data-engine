from __future__ import annotations

from data_engine.domain import FlowCatalogEntry, FlowCatalogState


def _entries() -> tuple[FlowCatalogEntry, ...]:
    return (
        FlowCatalogEntry(
            name="poller",
            group="Imports",
            title="Claims Poller",
            description="Polls for claims.",
            source_root="/tmp/input",
            target_root="/tmp/output",
            mode="poll",
            interval="30s",
            operations="Read -> Write",
            operation_items=("Read", "Write"),
            state="poll ready",
            valid=True,
            category="automated",
        ),
        FlowCatalogEntry(
            name="manual_review",
            group="Manual",
            title="Manual Review",
            description="Manual validation.",
            source_root="/tmp/input",
            target_root="/tmp/output",
            mode="manual",
            interval="-",
            operations="Build",
            operation_items=("Build",),
            state="manual",
            valid=True,
            category="manual",
        ),
    )


def test_flow_catalog_state_selects_first_card_when_cards_arrive():
    state = FlowCatalogState.empty().with_entries(_entries())

    assert state.selected_flow_name == "poller"
    assert state.selected_entry is not None
    assert state.selected_entry.name == "poller"
    assert state.has_automated_flows is True


def test_flow_catalog_state_preserves_existing_selection_when_present():
    state = FlowCatalogState.empty().with_selected_flow_name("manual_review").with_entries(_entries())

    assert state.selected_flow_name == "manual_review"
    assert state.selected_entry is not None
    assert state.selected_entry.name == "manual_review"


def test_flow_catalog_state_tracks_flow_states_and_empty_message():
    state = FlowCatalogState.empty().with_entries(_entries())
    updated = state.with_flow_states({"poller": "polling", "manual_review": "manual"}).with_empty_message("No discoverable flows were found yet")

    assert updated.flow_states["poller"] == "polling"
    assert updated.empty_message == "No discoverable flows were found yet"
