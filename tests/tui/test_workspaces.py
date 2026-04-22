from __future__ import annotations

from pathlib import Path

import pytest
from data_engine.domain import FlowLogEntry, RuntimeStepEvent
from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.ui.tui.app import RunGroupListItem
from data_engine.views.logs import FlowLogStore

from tests.tui.support import EmptyCatalogQueryService, FakeLogService, make_tui, resolve_workspace_paths, wait_for_tui_condition


@pytest.mark.anyio
async def test_tui_uses_local_workspace_collection_root_override(monkeypatch, tmp_path):
    override_root = tmp_path / "override_workspaces"
    (override_root / "example_workspace" / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(tmp_path / "data_engine"))

    store = LocalSettingsStore.open_default(app_root=Path(tmp_path / "data_engine"))
    store.set_workspace_collection_root(override_root)

    app = make_tui(settings_store=store)
    try:
        assert app.workspace_collection_root_override == override_root.resolve()
        assert app.workspace_paths.workspace_collection_root == override_root.resolve()
    finally:
        app.runtime_binding.runtime_cache_ledger.close()


@pytest.mark.anyio
async def test_tui_switching_workspaces_reloads_visible_log_runs(monkeypatch, tmp_path):
    workspace_collection_root = tmp_path / "workspaces"
    docs_root = workspace_collection_root / "docs"
    docs2_root = workspace_collection_root / "docs2"
    (docs_root / "flow_modules").mkdir(parents=True)
    (docs2_root / "flow_modules").mkdir(parents=True)

    initial_store = FlowLogStore()
    replacement_store = FlowLogStore()
    remove_calls: list[tuple[object, str]] = []
    app = make_tui(
        discover_workspaces_func=lambda app_root=None, workspace_collection_root=None, explicit_workspace_root=None: (
            type("DW", (), {"workspace_id": "docs", "workspace_root": docs_root})(),
            type("DW", (), {"workspace_id": "docs2", "workspace_root": docs2_root})(),
        ),
        resolve_workspace_paths_func=lambda workspace_id=None, workspace_root=None, workspace_collection_root=None, data_root=None: resolve_workspace_paths(
            workspace_root=docs_root if workspace_id in (None, "docs") else docs2_root,
            workspace_id="docs" if workspace_id in (None, "docs") else "docs2",
        ),
        log_service=FakeLogService(stores=(initial_store, replacement_store)),
    )
    async with app.run_test() as pilot:
        original_remove_client_session = app.runtime_binding_service.remove_client_session

        def _record_remove_client_session(binding, client_id):
            remove_calls.append((binding, client_id))
            return original_remove_client_session(binding, client_id)

        app.runtime_binding_service.remove_client_session = _record_remove_client_session
        flow_name = app.flow_cards[0].name
        app.selected_flow_name = flow_name
        app.log_store.append_entry(
            FlowLogEntry(
                line="run-docs",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-docs",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs.xlsx",
                    status="success",
                    elapsed_seconds=0.3,
                ),
            )
        )
        app._render_selected_flow()

        run_list = app.query_one("#log-run-list")
        initial_groups = app.log_store.runs_for_flow(flow_name)
        assert len(initial_groups) == 1
        assert [group.source_label for group in initial_groups] == ["docs.xlsx"]
        assert len([child for child in run_list.children if isinstance(child, RunGroupListItem)]) == 1

        replacement_store.append_entry(
            FlowLogEntry(
                line="run-docs2-a",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-docs2-a",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs2_a.xlsx",
                    status="success",
                    elapsed_seconds=0.4,
                ),
            )
        )
        replacement_store.append_entry(
            FlowLogEntry(
                line="run-docs2-b",
                kind="flow",
                flow_name=flow_name,
                event=RuntimeStepEvent(
                    run_id="run-docs2-b",
                    flow_name=flow_name,
                    step_name=None,
                    source_label="docs2_b.xlsx",
                    status="failed",
                    elapsed_seconds=0.6,
                ),
            )
        )
        app._switch_workspace("docs2")
        await wait_for_tui_condition(
            pilot,
            lambda: len([child for child in run_list.children if isinstance(child, RunGroupListItem)]) == 2,
        )

        switched_groups = app.log_store.runs_for_flow(flow_name)
        assert app.workspace_paths.workspace_id == "docs2"
        assert app.log_store is replacement_store
        assert remove_calls == []
        assert len(switched_groups) == 2
        assert [group.source_label for group in switched_groups] == ["docs2_a.xlsx", "docs2_b.xlsx"]
        assert len([child for child in run_list.children if isinstance(child, RunGroupListItem)]) == 2


@pytest.mark.anyio
async def test_tui_empty_workspace_reload_clears_stale_flow_rows():
    app = make_tui(log_service=FakeLogService())
    async with app.run_test() as pilot:
        list_view = app.query_one("#flow-list")
        initial_count = len(list_view.children)
        assert initial_count > 0
        app.catalog_query_service = EmptyCatalogQueryService()
        app.flow_controller.workspace.catalog_query_service = app.catalog_query_service

        app._load_flows()
        await wait_for_tui_condition(pilot, lambda: len(list_view.children) == 0)

        assert app.selected_flow_name is None
        assert len(list_view.children) == 0

