from __future__ import annotations

import pytest

from data_engine.authoring.flow import Flow
from data_engine.core.model import FlowValidationError
from data_engine.domain import FlowCatalogEntry
from data_engine.services.flow_catalog import FlowCatalogService, flow_catalog_entry_from_flow
from data_engine.services.logs import LogService
from data_engine.services.operator_queries import CatalogQueryService, HistoryQueryService


def test_catalog_query_service_returns_catalog_items_and_preview_rows(tmp_path):
    source = tmp_path / "input"
    source.mkdir()

    def _discover_definitions(*, data_root):
        del data_root
        return (
            type(
                "_Definition",
                (),
                {
                    "name": "docs_poll",
                    "description": "Poll docs",
                    "build": staticmethod(
                        lambda: Flow(name="docs_poll", group="Docs", label="Docs Poll")
                        .watch(mode="poll", source=source, interval="5s", settle=2, max_parallel=4)
                        .step(lambda context: context.current)
                    ),
                },
            )(),
        )

    flow_catalog_service = FlowCatalogService(discover_definitions_func=_discover_definitions)
    query_service = CatalogQueryService(flow_catalog_service=flow_catalog_service)

    items = query_service.list_flows(workspace_root=tmp_path)

    assert len(items) == 1
    assert items[0].flow_name == "docs_poll"
    assert items[0].group_name == "Docs"
    assert items[0].runtime_kind == "poll"
    assert items[0].settle == 2
    assert items[0].max_parallel == 4

    card = flow_catalog_service.load_entries(workspace_root=tmp_path)[0]
    preview = query_service.get_flow_preview(card=card, flow_states={"docs_poll": "polling"})

    assert preview.flow_name == "docs_poll"
    assert ("Settle", "2") in preview.rows
    assert ("State", "polling") in preview.rows
    assert ("Max Parallel", "4") in preview.rows


def test_history_query_service_returns_group_summaries_step_details_and_logs():
    created_at = "2026-04-16T12:00:00+00:00"
    step_rows = (
        type(
            "_StepRun",
            (),
            {
                "step_label": "Read Excel",
                "status": "success",
                "elapsed_seconds": 1.5,
                "output_path": "C:/tmp/out.parquet",
                "error_text": None,
            },
        )(),
    )
    log_entries = (
        type(
            "_PersistedLog",
            (),
            {
                "id": 1,
                "message": "run=run-1 flow=docs_poll source=C:/tmp/in.xlsx status=started",
                "flow_name": "docs_poll",
                "created_at_utc": created_at,
            },
        )(),
        type(
            "_PersistedLog",
            (),
            {
                "id": 2,
                "message": "run=run-1 flow=docs_poll step=Read Excel source=C:/tmp/in.xlsx status=success elapsed=1.5",
                "flow_name": "docs_poll",
                "created_at_utc": created_at,
            },
        )(),
        type(
            "_PersistedLog",
            (),
            {
                "id": 3,
                "message": "run=run-1 flow=docs_poll source=C:/tmp/in.xlsx status=success elapsed=2.5",
                "flow_name": "docs_poll",
                "created_at_utc": created_at,
            },
        )(),
    )

    class _LogLedger:
        def list(self, *, after_id=None):
            del after_id
            return log_entries

    store = LogService().create_store(type("_Ledger", (), {"logs": _LogLedger()})())
    history = HistoryQueryService(log_service=LogService())

    summaries = history.list_run_groups(store, flow_name="docs_poll")

    assert len(summaries) == 1
    assert summaries[0].run_id == "run-1"
    assert summaries[0].state == "success"
    assert summaries[0].source_label == "in.xlsx"

    ledger = type("_Ledger", (), {"step_outputs": type("_StepRepo", (), {"list_for_run": lambda self, run_id: step_rows})()})()
    steps = history.get_run_steps(ledger, run_id="run-1")

    assert len(steps) == 1
    assert steps[0].step_name == "Read Excel"
    assert steps[0].state == "success"
    assert steps[0].output_path == "C:/tmp/out.parquet"

    logs = history.get_run_logs(store, run_id="run-1", flow_name="docs_poll")

    assert [entry.run_id for entry in logs] == ["run-1", "run-1", "run-1"]
    assert logs[-1].text.endswith("success  in.xlsx")


def test_flow_catalog_entry_from_flow_builds_expected_metadata():
    flow = Flow(name="daily_summary", group="Docs").step(lambda context: context, label="Read Docs")
    entry = flow_catalog_entry_from_flow(flow, description="Loads docs")

    assert entry == FlowCatalogEntry(
        name="daily_summary",
        group="Docs",
        title="Daily Summary",
        description="Loads docs",
        source_root="(not set)",
        target_root="(not set)",
        mode="manual",
        interval="-",
        settle="-",
        operations="Read Docs",
        operation_items=("Read Docs",),
        state="manual",
        valid=True,
        category="manual",
    )


def test_flow_catalog_service_loads_and_sorts_entries_and_marks_invalid(tmp_path):
    good_flow = Flow(name="beta", group="Docs").step(lambda context: context, label="Good")

    class _Definition:
        def __init__(self, name, description, builder):
            self.name = name
            self.description = description
            self._builder = builder

        def build(self):
            return self._builder()

    defs = (
        _Definition("zeta", "broken", lambda: (_ for _ in ()).throw(RuntimeError("boom"))),
        _Definition("beta", "good", lambda: good_flow),
    )
    service = FlowCatalogService(discover_definitions_func=lambda **kwargs: defs)

    entries = service.load_entries(workspace_root=tmp_path)

    assert [entry.name for entry in entries] == ["beta", "zeta"]
    assert entries[0].valid is True
    assert entries[1].valid is False
    assert entries[1].error == "boom"

    empty_service = FlowCatalogService(discover_definitions_func=lambda **kwargs: ())
    with pytest.raises(FlowValidationError, match="No flow modules discovered"):
        empty_service.load_entries(workspace_root=tmp_path)

