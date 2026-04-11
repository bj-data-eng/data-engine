from __future__ import annotations

import json
from pathlib import Path
from queue import Queue
import threading
import time
from textwrap import dedent

import duckdb
import polars as pl

from data_engine.authoring.flow import Flow, load_flow
from data_engine.core.model import FlowStoppedError
from data_engine.core.primitives import Batch
from data_engine.flow_modules.flow_module_loader import discover_flow_module_definitions, load_flow_module_definition
from data_engine.hosts.scheduler import SchedulerHost
from data_engine.runtime.engine import RuntimeEngine
from data_engine.runtime.execution import _FlowRuntime, _GroupedFlowRuntime
from data_engine.runtime.runtime_db import RuntimeLedger, utcnow_text
from data_engine.services import FlowCatalogService, FlowExecutionService
from data_engine.views.models import qt_flow_cards_from_entries


def _write_workspace_flow_module(workspace_root: Path, name: str, source: str) -> None:
    """Write one authored Python flow module into a temporary workspace."""
    flow_modules_dir = workspace_root / "flow_modules"
    flow_modules_dir.mkdir(parents=True, exist_ok=True)
    (flow_modules_dir / f"{name}.py").write_text(dedent(source).strip() + "\n", encoding="utf-8")


def _build_workspace_surface(workspace_root: Path) -> None:
    """Create a representative authored workspace surface for integration tests."""
    _write_workspace_flow_module(
        workspace_root,
        "claims_demo",
        """
        from data_engine import Flow

        DESCRIPTION = "Poll a claims folder and mirror outputs."

        def build():
            return (
                Flow(name="claims_demo", group="Claims")
                .watch(mode="poll", source="../../../data/Input/claims_flat", interval="5s", extensions=[".xlsx"])
                .mirror(root="../../../data/Output/claims_demo")
                .step(lambda context: context.current, label="Read Claims")
            )
        """,
    )
    _write_workspace_flow_module(
        workspace_root,
        "claims_poll",
        """
        from data_engine import Flow

        def build():
            return (
                Flow(name="claims_poll", group="Claims")
                .watch(mode="poll", source="../../../data/Input/claims_dated", interval="5s", extensions=[".xlsx"])
                .mirror(root="../../../data/Output/claims_poll")
                .step(lambda context: context.current, label="Read Claims")
            )
        """,
    )
    _write_workspace_flow_module(
        workspace_root,
        "claims_summary",
        """
        from data_engine import Flow

        def build():
            return (
                Flow(name="claims_summary", group="Analytics")
                .watch(mode="schedule", run_as="batch", time=["08:00", "12:00"], source="../../../data/Input/claims_flat")
                .mirror(root="../../../data/Output/claims_summary")
                .step(lambda context: context.current, label="Build Summary")
            )
        """,
    )
    _write_workspace_flow_module(
        workspace_root,
        "daily_settings",
        """
        from data_engine import Flow

        def build():
            return (
                Flow(name="daily_settings", group="Settings")
                .watch(mode="schedule", run_as="batch", time=["09:00", "17:00"], source="../../../data/Settings/single_watch.xlsx")
                .mirror(root="../../../data/Output/daily_settings")
                .step(lambda context: context.current, label="Read Settings")
            )
        """,
    )
    _write_workspace_flow_module(
        workspace_root,
        "long_step_demo",
        """
        from data_engine import Flow

        def build():
            return Flow(name="long_step_demo", label="Long Step Demo", group="Manual").step(lambda context: context.current, label="Stage One")
        """,
    )
    _write_workspace_flow_module(
        workspace_root,
        "manual_claims_demo",
        """
        from data_engine import Flow

        def build():
            return Flow(name="manual_claims_demo", label="Manual Claims Demo", group="Manual").step(lambda context: context.current, label="Read Claims")
        """,
    )


def test_workspace_starter_flows_are_discoverable(tmp_path):
    workspace_root = tmp_path / "workspace"
    _build_workspace_surface(workspace_root)
    definitions = discover_flow_module_definitions(data_root=workspace_root)
    names = [definition.name for definition in definitions]

    assert "claims_demo" in names
    assert "claims_poll" in names
    assert "daily_settings" in names
    assert "manual_claims_demo" in names


def test_loaded_flow_exposes_workspace_toml_config_in_context(tmp_path):
    workspace_root = tmp_path / "workspace"
    (workspace_root / "config").mkdir(parents=True)
    (workspace_root / "config" / "claims.toml").write_text(
        """
        [runtime]
        batch_size = 7000
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    _write_workspace_flow_module(
        workspace_root,
        "claims_config",
        """
        from data_engine import Flow

        def build():
            return Flow(name="claims_config", group="Claims").step(
                lambda context: context.config.require("claims")["runtime"]["batch_size"]
            )
        """,
    )

    flow = load_flow_module_definition("claims_config", data_root=workspace_root).build()
    result = _FlowRuntime((flow,), continuous=False).run()[0]

    assert result.current == 7000


def test_load_flow_and_ui_cards_match_workspace_surface(tmp_path):
    workspace_root = tmp_path / "workspace"
    _build_workspace_surface(workspace_root)
    catalog_service = FlowCatalogService()
    execution_service = FlowExecutionService()
    flow = load_flow("claims_demo", data_root=workspace_root)
    cards = {
        card.name: card
        for card in qt_flow_cards_from_entries(catalog_service.load_entries(workspace_root=workspace_root))
    }

    assert flow.name == "claims_demo"
    assert flow.mode == "poll"
    assert len(cards) == 6
    assert cards["claims_demo"].mode == "poll"
    assert cards["daily_settings"].mode == "schedule"
    assert cards["long_step_demo"].mode == "manual"
    assert cards["manual_claims_demo"].mode == "manual"
    assert cards["manual_claims_demo"].source_root == "(not set)"
    assert cards["daily_settings"].source_root.endswith("single_watch.xlsx")
    assert cards["claims_summary"].target_root.endswith("claims_summary")
    assert execution_service.load_flow("claims_demo", workspace_root=workspace_root).name == "claims_demo"


def test_generic_polled_flow_runs_end_to_end_with_native_polars_io(tmp_path):
    source = tmp_path / "claims.parquet"
    target_root = tmp_path / "output"
    target = target_root / "claims.parquet"
    pl.DataFrame({"status": ["OPEN", "DONE"], "value": [1, 2]}).write_parquet(source)

    def read_source(context):
        return pl.read_parquet(context.source.path)

    def keep_open(context):
        return context.current.filter(pl.col("status") == "OPEN")

    def write_target(context):
        output = context.mirror.with_suffix(".parquet")
        context.current.write_parquet(output)
        return output

    results = (
        Flow(name="claims_poll", group="Claims")
        .watch(mode="poll", source=source, interval="5s")
        .mirror(root=target_root)
        .step(read_source, save_as="raw_df", label="Read Parquet")
        .step(keep_open, use="raw_df", save_as="filtered_df", label="Keep Open")
        .step(write_target, use="filtered_df", label="Write Parquet")
        .run_once()
    )

    assert len(results) == 1
    assert target.exists()
    assert pl.read_parquet(target).to_dict(as_series=False) == {"status": ["OPEN"], "value": [1]}
    assert results[0].metadata["step_outputs"]["Write Parquet"] == target.resolve()


def test_grouped_runtime_keeps_groups_sequential_and_independent():
    order: list[str] = []

    def mark(label: str):
        def _inner(context):
            order.append(label)
            return context.current

        return _inner

    runtime = _GroupedFlowRuntime(
        (
            Flow(name="a1", group="alpha").step(mark("a1")),
            Flow(name="a2", group="alpha").step(mark("a2")),
            Flow(name="b1", group="beta").step(mark("b1")),
        ),
        continuous=False,
    )

    runtime.run()

    assert order.index("a1") < order.index("a2")
    assert set(order) == {"a1", "a2", "b1"}


def test_temporary_workspace_flow_modules_compile_and_load_from_notebooks(tmp_path):
    workspace = tmp_path / "workspace"
    flow_modules_dir = workspace / "flow_modules"
    flow_modules_dir.mkdir(parents=True)
    notebook_path = flow_modules_dir / "demo.ipynb"
    notebook_path.write_text(
        json.dumps(
            {
                "cells": [
                    {
                        "cell_type": "code",
                        "metadata": {},
                        "source": [
                            "from data_engine import Flow\n",
                            'DESCRIPTION = "Temporary compiled flow"\n',
                            "def build():\n",
                            '    return Flow(name="demo", label="Demo", group="Tests").step(lambda context: context.current)\n',
                        ],
                    }
                ],
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 5,
            }
        ),
        encoding="utf-8",
    )

    definition = load_flow_module_definition("demo", data_root=workspace)
    flow = definition.build()
    discovered = discover_flow_module_definitions(data_root=workspace)

    assert flow.name == "demo"
    assert flow.group == "Tests"
    assert [item.name for item in discovered] == ["demo"]


def test_directory_poll_processes_many_files_end_to_end(tmp_path):
    source_dir = tmp_path / "input"
    target_dir = tmp_path / "output"
    source_dir.mkdir()
    target_dir.mkdir()

    for idx in range(25):
        pl.DataFrame({"file": [idx], "value": [idx * 2]}).write_parquet(source_dir / f"claims_{idx}.parquet")

    def read_source(context):
        return pl.read_parquet(context.source.path)

    def write_target(context):
        output = context.mirror.with_suffix(".parquet")
        context.current.write_parquet(output)
        return output

    results = (
        Flow(name="many_files", group="Claims")
        .watch(mode="poll", source=source_dir, interval="5s", extensions=[".parquet"])
        .mirror(root=target_dir)
        .step(read_source, label="Read Parquet")
        .step(write_target, label="Write Parquet")
        .run_once()
    )

    assert len(results) == 25
    assert len(list(target_dir.glob("*.parquet"))) == 25


def test_directory_poll_filters_mixed_file_types(tmp_path):
    source_dir = tmp_path / "input"
    target_dir = tmp_path / "output"
    source_dir.mkdir()
    target_dir.mkdir()

    pl.DataFrame({"kind": ["parquet"]}).write_parquet(source_dir / "claims.parquet")
    (source_dir / "notes.txt").write_text("ignore", encoding="utf-8")
    (source_dir / "claims.xlsx").write_text("not really excel", encoding="utf-8")

    seen: list[str] = []

    def read_source(context):
        seen.append(context.source.path.name)
        return pl.read_parquet(context.source.path)

    def write_target(context):
        output = context.mirror.with_suffix(".parquet")
        context.current.write_parquet(output)
        return output

    results = (
        Flow(name="mixed_types", group="Claims")
        .watch(mode="poll", source=source_dir, interval="5s", extensions=[".parquet"])
        .mirror(root=target_dir)
        .step(read_source)
        .step(write_target)
        .run_once()
    )

    assert len(results) == 1
    assert seen == ["claims.parquet"]


def test_directory_poll_staleness_uses_runtime_ledger_not_output_timestamps(tmp_path):
    source_dir = tmp_path / "input"
    target_dir = tmp_path / "output"
    nested_source = source_dir / "team_a"
    nested_target = target_dir / "team_a"
    nested_source.mkdir(parents=True)
    nested_target.mkdir(parents=True)

    source = nested_source / "claims.xlsx"
    source.write_text("placeholder", encoding="utf-8")
    target = nested_target / "claims.parquet"
    target.write_text("done", encoding="utf-8")

    flow = Flow(name="claims_demo", group="Claims").watch(
        mode="poll",
        source=source_dir,
        interval="5s",
        extensions=[".xlsx"],
    ).mirror(root=target_dir).step(lambda context: context.current)

    runtime = _FlowRuntime((flow,), continuous=True)
    signature = runtime.runtime_ledger.source_signature_for_path(source)

    assert signature is not None
    assert runtime._stale_poll_sources(flow) == [source]

    runtime.runtime_ledger.upsert_file_state(
        flow_name="claims_demo",
        signature=signature,
        status="success",
        run_id="run-1",
        finished_at_utc=utcnow_text(),
    )

    assert runtime._stale_poll_sources(flow) == []


def test_directory_poll_assigns_distinct_run_ids_per_source_execution(tmp_path):
    source_dir = tmp_path / "input"
    target_dir = tmp_path / "output"
    source_dir.mkdir()
    target_dir.mkdir()
    pl.DataFrame({"value": [1]}).write_parquet(source_dir / "a.parquet")
    pl.DataFrame({"value": [2]}).write_parquet(source_dir / "b.parquet")

    def read_source(context):
        return pl.read_parquet(context.source.path)

    def write_target(context):
        output = context.mirror.with_suffix(".parquet")
        context.current.write_parquet(output)
        return output

    Flow(name="claims_demo", group="Claims").watch(
        mode="poll",
        source=source_dir,
        interval="5s",
        extensions=[".parquet"],
    ).mirror(root=target_dir).step(read_source).step(write_target).run_once()

    runs = RuntimeLedger.open_default().list_runs(flow_name="claims_demo")

    assert len(runs) == 2
    assert len({run.run_id for run in runs}) == 2
    assert {Path(run.source_path).name for run in runs if run.source_path is not None} == {"a.parquet", "b.parquet"}


def test_scheduled_flow_can_create_duckdb_in_missing_output_directory(tmp_path):
    source_dir = tmp_path / "input"
    target_file = tmp_path / "output" / "claims_summary" / "workflow_summary.parquet"
    source_dir.mkdir(parents=True)
    pl.DataFrame({"Workflow": ["A", "A", "B"]}).write_parquet(source_dir / "claims.parquet")

    def read_source(context):
        return pl.read_parquet(context.source.root_file("claims.parquet"))

    def build_summary(context):
        conn = duckdb.connect(context.mirror.file("analytics.duckdb"))
        try:
            conn.register("input", context.current)
            return conn.sql(
                """
                select Workflow, count(*) as row_count
                from input
                group by Workflow
                order by row_count desc
                """
            ).pl()
        finally:
            conn.close()

    def write_summary(context):
        output = context.mirror.file("workflow_summary.parquet")
        context.current.write_parquet(output)
        return output

    result = (
        Flow(name="claims_summary", group="Analytics")
        .watch(mode="schedule", run_as="batch", interval="15m", source=source_dir)
        .mirror(root=target_file.parent)
        .step(read_source, save_as="raw_df")
        .step(build_summary, use="raw_df", save_as="summary_df")
        .step(write_summary, use="summary_df")
        .run_once()[0]
    )

    assert (target_file.parent / "analytics.duckdb").exists()
    assert target_file.exists()
    assert isinstance(result.current, Path)


def test_collect_and_map_support_pdf_style_batch_workflows(tmp_path):
    source_dir = tmp_path / "pdfs"
    source_dir.mkdir()
    (source_dir / "ok_a.pdf").write_text("ok", encoding="utf-8")
    (source_dir / "ok_b.pdf").write_text("ok", encoding="utf-8")

    def validate_pdf(file_ref):
        return {"name": file_ref.name, "path": file_ref.path, "ok": file_ref.path.suffix == ".pdf"}

    def collect_valid_names(context):
        return tuple(item["name"] for item in context.current if item["ok"])

    result = (
        Flow(name="pdf_batch_flow", group="Claims")
        .watch(mode="schedule", run_as="batch", interval="15m", source=source_dir)
        .collect([".pdf"], save_as="pdf_files")
        .map(validate_pdf, use="pdf_files", save_as="pdf_results")
        .step(collect_valid_names, use="pdf_results")
        .run_once()[0]
        .current
    )

    assert result == ("ok_a.pdf", "ok_b.pdf")


def test_single_file_poll_missing_source_is_treated_as_stale(tmp_path):
    missing_source = tmp_path / "Settings" / "single_watch.xlsx"
    target_root = tmp_path / "output"

    flow = (
        Flow(name="single_watch_demo", group="Settings")
        .watch(mode="poll", source=missing_source, interval="5s")
        .mirror(root=target_root)
        .step(lambda context: context.current)
    )

    runtime = _FlowRuntime((flow,), continuous=True)

    assert runtime._is_poll_source_stale(flow, missing_source) is True  # noqa: SLF001 - targeted runtime behavior


def test_collect_returns_empty_batch_when_directory_has_no_matches(tmp_path):
    source_dir = tmp_path / "empty"
    source_dir.mkdir()

    result = (
        Flow(name="empty_batch", group="Claims")
        .watch(mode="schedule", run_as="batch", interval="15m", source=source_dir)
        .collect([".pdf"])
        .run_once()[0]
        .current
    )

    assert isinstance(result, Batch)
    assert len(result) == 0


def test_large_parquet_flow_completes_without_runtime_timeout(tmp_path):
    source = tmp_path / "large.parquet"
    target_root = tmp_path / "output"
    target = target_root / "large.parquet"
    row_count = 50_000
    pl.DataFrame(
        {
            "status": ["OPEN"] * row_count,
            "value": list(range(row_count)),
        }
    ).write_parquet(source)

    def read_source(context):
        return pl.read_parquet(context.source.path)

    def summarize(context):
        return context.current.select(pl.len().alias("row_count"), pl.col("value").sum().alias("value_sum"))

    def write_target(context):
        output = context.mirror.with_suffix(".parquet")
        context.current.write_parquet(output)
        return output

    started = time.monotonic()
    results = (
        Flow(name="large_flow", group="Claims")
        .watch(mode="poll", source=source, interval="5s")
        .mirror(root=target_root)
        .step(read_source)
        .step(summarize)
        .step(write_target)
        .run_once()
    )
    elapsed = time.monotonic() - started

    assert len(results) == 1
    assert target.exists()
    assert pl.read_parquet(target).to_dict(as_series=False) == {
        "row_count": [row_count],
        "value_sum": [sum(range(row_count))],
    }
    assert elapsed >= 0.0


def test_long_running_step_can_be_canceled_cooperatively():
    started = threading.Event()
    release = threading.Event()
    stop_event = threading.Event()
    errors: "Queue[Exception]" = Queue()
    ran_next: list[str] = []

    def slow_step(context):
        started.set()
        release.wait(timeout=1.0)
        return "done"

    def next_step(context):
        ran_next.append("next")
        return context.current

    runtime = _FlowRuntime(
        (
            Flow(name="slow_flow", group="Claims")
            .step(slow_step, save_as="first")
            .step(next_step, use="first"),
        ),
        continuous=False,
        flow_stop_event=stop_event,
    )

    def _run():
        try:
            runtime.run()
        except Exception as exc:
            errors.put(exc)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    assert started.wait(timeout=1.0)
    stop_event.set()
    release.set()
    thread.join(timeout=2.0)

    exc = errors.get_nowait()
    assert isinstance(exc, FlowStoppedError)
    assert ran_next == []


def test_groups_run_in_parallel_but_keep_context_objects_isolated():
    alpha_started = threading.Event()
    beta_started = threading.Event()
    order: list[str] = []

    def alpha_step(context):
        order.append("alpha_start")
        alpha_started.set()
        beta_started.wait(timeout=1.0)
        return "alpha"

    def beta_step(context):
        order.append("beta_start")
        beta_started.set()
        alpha_started.wait(timeout=1.0)
        return "beta"

    runtime = _GroupedFlowRuntime(
        (
            Flow(name="alpha_flow", group="alpha").step(alpha_step, save_as="shared"),
            Flow(name="beta_flow", group="beta").step(beta_step, save_as="shared"),
        ),
        continuous=False,
    )

    results = runtime.run()
    by_name = {result.flow_name: result for result in results}

    assert "alpha_start" in order
    assert "beta_start" in order
    assert by_name["alpha_flow"].objects["shared"] == "alpha"
    assert by_name["beta_flow"].objects["shared"] == "beta"
    assert by_name["alpha_flow"].objects is not by_name["beta_flow"].objects


def test_scheduler_host_keeps_running_when_one_scheduled_flow_fails():
    runtime_stop = threading.Event()
    failing_count = 0
    healthy_count = 0

    def failing_step(context):
        nonlocal failing_count
        failing_count += 1
        if healthy_count >= 2:
            runtime_stop.set()
        raise RuntimeError("boom")

    def healthy_step(context):
        nonlocal healthy_count
        healthy_count += 1
        if failing_count >= 1 and healthy_count >= 2:
            runtime_stop.set()
        return healthy_count

    flows = (
        Flow(name="failing_flow", group="alpha").watch(mode="schedule", run_as="batch", interval="50ms").step(failing_step),
        Flow(name="healthy_flow", group="beta").watch(mode="schedule", run_as="batch", interval="50ms").step(healthy_step),
    )
    engine = RuntimeEngine(runtime_ledger=RuntimeLedger.open_default())
    scheduler_host = SchedulerHost(runtime_engine=engine)

    jobs = scheduler_host.run_until_stopped(flows, runtime_stop)
    ledger = RuntimeLedger.open_default()
    failing_runs = ledger.list_runs(flow_name="failing_flow")
    healthy_runs = ledger.list_runs(flow_name="healthy_flow")

    assert healthy_count >= 2
    assert len(jobs) == 2
    assert any(run.status == "failed" for run in failing_runs)
    assert len([run for run in healthy_runs if run.status == "success"]) >= 2
