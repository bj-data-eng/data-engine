from __future__ import annotations

import hashlib
from pathlib import Path
import threading

import polars as pl
import pytest

from data_engine.authoring.builder import (
    Batch,
    FileRef,
    Flow,
    FlowContext,
    MirrorContext,
    SourceContext,
    _FlowRuntime,
    _GroupedFlowRuntime,
    discover_flows,
    load_flow,
    run,
)
from data_engine.authoring.model import FlowValidationError
from data_engine.flow_modules.flow_module_loader import compiled_flow_module_context
from data_engine.runtime.runtime_db import RuntimeLedger


def test_flow_requires_non_empty_name_group_and_label():
    with pytest.raises(FlowValidationError, match="group"):
        Flow(group="")

    with pytest.raises(FlowValidationError, match="when provided"):
        Flow(group="Claims", name="")

    with pytest.raises(FlowValidationError, match="label"):
        Flow(group="Claims", label="")


def test_flow_label_is_distinct_from_internal_name():
    flow = Flow(name="claims_summary", label="Claims Summary", group="Claims")

    assert flow.name == "claims_summary"
    assert flow.label == "Claims Summary"


def test_runtime_uniqueness_remains_based_on_internal_flow_name_not_label():
    first = Flow(name="name_name", group="Claims")
    second = Flow(name="NameName", group="Claims")

    runtime = _FlowRuntime((first.step(lambda context: context.current), second.step(lambda context: context.current)), continuous=False)

    assert runtime._validate() is None


def test_watch_validates_poll_single_file_and_directory_modes(tmp_path):
    source_file = tmp_path / "claims.xlsx"
    source_dir = tmp_path / "input"
    source_dir.mkdir()

    built = Flow(name="claims", group="Claims").watch(
        mode="poll",
        source=source_file,
        interval="5s",
    )
    assert built.trigger is not None
    assert built.trigger.mode == "poll"
    assert built.trigger.run_as == "individual"
    assert built.trigger.source == source_file.resolve()

    directory_built = Flow(name="claims_dir", group="Claims").watch(
        mode="poll",
        source=source_dir,
        interval="5s",
        extensions=["xlsx", ".xlsm"],
        settle=2,
    )
    assert directory_built.trigger is not None
    assert directory_built.trigger.source == source_dir.resolve()
    assert directory_built.trigger.extensions == (".xlsx", ".xlsm")
    assert directory_built.trigger.settle == 2


def test_watch_validates_schedule_interval_and_times():
    built = Flow(name="every_flow", group="Claims").watch(
        mode="schedule",
        source="/tmp/in",
        interval="10m",
    )
    assert built.trigger is not None
    assert built.trigger.mode == "schedule"
    assert built.trigger.interval_seconds == 600.0
    assert built.trigger.source is not None

    at_built = Flow(name="at_flow", group="Claims").watch(mode="schedule", time="10:31")
    assert at_built.trigger is not None
    assert at_built.trigger.times == ("10:31",)
    assert at_built.trigger.time_slots == ((10, 31),)

    multi_at_built = Flow(name="multi_at_flow", group="Claims").watch(mode="schedule", time={"14:45", "08:15", "14:45"})
    assert multi_at_built.trigger is not None
    assert multi_at_built.trigger.times == ("08:15", "14:45")
    assert multi_at_built.trigger.time_slots == ((8, 15), (14, 45))

    with pytest.raises(FlowValidationError, match="exactly one"):
        Flow(name="bad", group="Claims").watch(mode="schedule")

    with pytest.raises(FlowValidationError, match="exactly one"):
        Flow(name="bad2", group="Claims").watch(mode="schedule", interval="5m", time="10:31")

    with pytest.raises(FlowValidationError, match="time must include at least one time"):
        Flow(name="bad3", group="Claims").watch(mode="schedule", time=[])


def test_watch_manual_allows_source_and_batch_directory_context(tmp_path):
    source_dir = tmp_path / "input"
    source_dir.mkdir()
    (source_dir / "a.xlsx").write_text("a", encoding="utf-8")
    (source_dir / "b.xlsx").write_text("b", encoding="utf-8")

    result = (
        Flow(name="manual_batch", group="Claims")
        .watch(mode="manual", source=source_dir, run_as="batch", extensions=[".xlsx"])
        .step(
            lambda context: {
                "source_path": context.source.path,
                "source_root": context.source.root,
            }
        )
        .run_once()
    )

    assert len(result) == 1
    assert result[0].current["source_path"] is None
    assert result[0].current["source_root"] == source_dir.resolve()


def test_watch_manual_individual_iterates_directory_files(tmp_path):
    source_dir = tmp_path / "input"
    source_dir.mkdir()
    (source_dir / "b.xlsx").write_text("b", encoding="utf-8")
    (source_dir / "a.xlsx").write_text("a", encoding="utf-8")
    (source_dir / "skip.txt").write_text("x", encoding="utf-8")

    results = (
        Flow(name="manual_individual", group="Claims")
        .watch(mode="manual", source=source_dir, extensions=[".xlsx"])
        .step(lambda context: context.source.path.name)
        .run_once()
    )

    assert [item.current for item in results] == ["a.xlsx", "b.xlsx"]



def test_step_requires_callable_and_normalizes_labels():
    class ClaimsCleaner:
        def __call__(self, context):
            return context.current

    flow = Flow(name="claims", group="Claims").step(ClaimsCleaner())
    assert flow.steps[0].label == "Claims Cleaner"

    explicit = Flow(name="claims2", group="Claims").step(lambda context: context.current, label="Keep Current")
    assert explicit.steps[0].label == "Keep Current"
    assert explicit.steps[0].function_name == "<lambda>"

    with pytest.raises(FlowValidationError, match="callable"):
        Flow(name="bad", group="Claims").step("nope")

    with pytest.raises(FlowValidationError, match="save_as cannot overwrite"):
        Flow(name="bad_save", group="Claims").step(lambda context: context.current, save_as="current")


def test_failed_step_records_step_label_and_function_name(tmp_path):
    source = tmp_path / "claims.parquet"
    pl.DataFrame({"value": [1]}).write_parquet(source)

    def read_claims(context):
        return pl.read_parquet(context.source.path)

    def explode_claims(context):
        raise RuntimeError("boom")

    flow = (
        Flow(name="claims_poll", group="Claims")
        .watch(mode="poll", source=source, interval="5s")
        .step(read_claims, label="Read Claims")
        .step(explode_claims, label="Explode Claims")
    )

    with pytest.raises(FlowValidationError, match='Flow "claims_poll" failed in step "Explode Claims"'):
        flow.run_once()

    run = RuntimeLedger.open_default().list_runs(flow_name="claims_poll")[0]
    assert 'function explode_claims' in str(run.error_text)


def test_runtime_requires_flow_names_before_execution():
    with pytest.raises(FlowValidationError, match="must be set before execution"):
        Flow(group="Claims").step(lambda context: context.current).run_once()


def test_run_once_updates_current_and_saved_objects():
    def build_dataframe(context):
        return pl.DataFrame({"status": ["OPEN", "DONE"], "value": [1, 2]})

    def keep_open(context):
        return context.current.filter(pl.col("status") == "OPEN")

    results = (
        Flow(name="manual_report", group="Reports")
        .step(build_dataframe, save_as="raw_df")
        .step(keep_open, use="raw_df", save_as="filtered_df")
        .run_once()
    )

    assert len(results) == 1
    context = results[0]
    assert isinstance(context.current, pl.DataFrame)
    assert context.current.height == 1
    assert context.objects["raw_df"].height == 2
    assert context.objects["filtered_df"].height == 1


def test_flow_context_mirror_prepares_write_ready_paths(tmp_path):
    flow = (
        Flow(name="claims", group="Claims")
        .watch(mode="poll", source=tmp_path / "input" / "report.xlsx", interval="5s")
        .mirror(root=tmp_path / "output")
        .step(lambda context: context.mirror.with_suffix(".parquet"))
    )
    flow.trigger.source.parent.mkdir(parents=True)
    flow.trigger.source.write_text("x", encoding="utf-8")

    output = flow.run_once()[0].current

    assert output == (tmp_path / "output" / "report.parquet").resolve()
    assert output.parent.exists()


def test_flow_context_mirror_file_targets_mirrored_folder_without_namespacing(tmp_path):
    flow = (
        Flow(name="claims", group="Claims")
        .watch(mode="poll", source=tmp_path / "input" / "nested" / "report.xlsx", interval="5s")
        .mirror(root=tmp_path / "output")
        .step(lambda context: context.mirror.file("open_claims.parquet"))
    )
    flow.trigger.source.parent.mkdir(parents=True)
    flow.trigger.source.write_text("x", encoding="utf-8")

    output = flow.run_once()[0].current

    assert output == (tmp_path / "output" / "open_claims.parquet").resolve()
    assert output.parent.exists()


def test_flow_context_mirror_namespaced_file_uses_source_stem_namespace(tmp_path):
    flow = (
        Flow(name="claims", group="Claims")
        .watch(mode="poll", source=tmp_path / "input" / "nested" / "report.xlsx", interval="5s")
        .mirror(root=tmp_path / "output")
        .step(lambda context: context.mirror.namespaced_file("open_claims.parquet"))
    )
    flow.trigger.source.parent.mkdir(parents=True)
    flow.trigger.source.write_text("x", encoding="utf-8")

    output = flow.run_once()[0].current

    assert output == (tmp_path / "output" / "report" / "open_claims.parquet").resolve()
    assert output.parent.exists()


def test_flow_context_mirror_with_suffix_requires_concrete_source():
    with pytest.raises(FlowValidationError, match="concrete source file"):
        MirrorContext(root="/tmp/output").with_suffix(".parquet")

    with pytest.raises(FlowValidationError, match="concrete source file"):
        MirrorContext(root="/tmp/output").with_extension(".parquet")


def test_source_context_with_suffix_and_file_resolve_relative_paths(tmp_path):
    source = SourceContext(
        root=tmp_path / "input",
        path=tmp_path / "input" / "nested" / "report.xlsx",
        relative_path=Path("nested/report.xlsx"),
    )

    assert source.with_suffix(".json") == (tmp_path / "input" / "nested" / "report.json").resolve()
    assert source.with_extension(".json") == (tmp_path / "input" / "nested" / "report.json").resolve()
    assert source.file("notes.json") == (tmp_path / "input" / "nested" / "notes.json").resolve()
    assert source.namespaced_file("notes.json") == (tmp_path / "input" / "nested" / "report" / "notes.json").resolve()
    assert source.root_file("lookup.csv") == (tmp_path / "input" / "lookup.csv").resolve()


def test_source_context_namespaced_file_requires_concrete_source(tmp_path):
    source = SourceContext(root=tmp_path / "input")

    with pytest.raises(FlowValidationError, match="concrete source file"):
        source.namespaced_file("notes.json")


def test_collect_files_returns_file_batch_with_name_and_path_access(tmp_path):
    source_dir = tmp_path / "input"
    source_dir.mkdir()
    (source_dir / "b.pdf").write_text("b", encoding="utf-8")
    (source_dir / "a.pdf").write_text("a", encoding="utf-8")
    (source_dir / "skip.txt").write_text("x", encoding="utf-8")

    result = (
        Flow(name="pdf_batch", group="Claims")
        .watch(mode="schedule", run_as="batch", interval="15m", source=source_dir)
        .collect([".pdf"])
        .run_once()[0]
        .current
    )

    assert isinstance(result, Batch)
    assert result.names() == ("a.pdf", "b.pdf")
    assert result.paths() == ((source_dir / "a.pdf").resolve(), (source_dir / "b.pdf").resolve())
    assert isinstance(result[0], FileRef)
    assert result[0].name == "a.pdf"
    assert result[0].path == (source_dir / "a.pdf").resolve()


def test_collect_files_root_resolves_relative_to_compiled_flow_module_dir(tmp_path):
    compiled_dir = tmp_path / "workspace" / "compiled_flow_modules"
    source_dir = tmp_path / "data" / "Input" / "claims_flat"
    compiled_dir.mkdir(parents=True)
    source_dir.mkdir(parents=True)
    (source_dir / "claims_a.pdf").write_text("a", encoding="utf-8")

    with compiled_flow_module_context(compiled_dir):
        flow = Flow(name="pdf_batch", group="Claims").collect([".pdf"], root="../../data/Input/claims_flat")

    result = flow.run_once()[0].current

    assert isinstance(result, Batch)
    assert result.names() == ("claims_a.pdf",)


def test_collect_files_preserves_resolved_root_after_compiled_build_context(tmp_path):
    compiled_dir = tmp_path / "workspace" / "compiled_flow_modules"
    source_dir = tmp_path / "data" / "Input" / "claims_flat"
    compiled_dir.mkdir(parents=True)
    source_dir.mkdir(parents=True)
    (source_dir / "claims_a.pdf").write_text("a", encoding="utf-8")

    with compiled_flow_module_context(compiled_dir):
        flow = Flow(name="pdf_batch", group="Claims").collect([".pdf"], root="../../data/Input/claims_flat")

    result = flow.run_once()[0].current

    assert isinstance(result, Batch)
    assert result.names() == ("claims_a.pdf",)


def test_step_each_maps_batch_items_without_raw_list_access(tmp_path):
    source_dir = tmp_path / "input"
    source_dir.mkdir()
    (source_dir / "good.pdf").write_text("ok", encoding="utf-8")
    (source_dir / "bad.pdf").write_text("broken", encoding="utf-8")

    def validate_pdf(file_ref: FileRef):
        return {"name": file_ref.name, "path": file_ref.path, "ok": file_ref.name != "bad.pdf"}

    result = (
        Flow(name="pdf_validation", group="Claims")
        .watch(mode="schedule", run_as="batch", interval="15m", source=source_dir)
        .collect([".pdf"])
        .map(validate_pdf, label="Validate Pdf")
        .run_once()[0]
        .current
    )

    assert isinstance(result, Batch)
    assert [item["name"] for item in result] == ["bad.pdf", "good.pdf"]
    assert [item["ok"] for item in result] == [False, True]


def test_step_each_can_use_context_and_item(tmp_path):
    source_dir = tmp_path / "input"
    source_dir.mkdir()
    (source_dir / "claims_a.pdf").write_text("a", encoding="utf-8")

    def annotate_file(context, file_ref: FileRef):
        return {
            "flow": context.flow_name,
            "name": file_ref.name,
            "path": file_ref.path,
        }

    result = (
        Flow(name="pdf_context", group="Claims")
        .watch(mode="schedule", run_as="batch", interval="15m", source=source_dir)
        .collect([".pdf"])
        .map(annotate_file)
        .run_once()[0]
        .current
    )

    assert isinstance(result, Batch)
    assert result[0]["flow"] == "pdf_context"
    assert result[0]["name"] == "claims_a.pdf"


def test_step_can_iterate_batch_directly_from_context_current(tmp_path):
    source_dir = tmp_path / "input"
    source_dir.mkdir()
    (source_dir / "claims_a.pdf").write_text("a", encoding="utf-8")
    (source_dir / "claims_b.pdf").write_text("b", encoding="utf-8")

    def summarize_batch(context):
        return tuple(file_ref.name for file_ref in context.current)

    result = (
        Flow(name="pdf_names", group="Claims")
        .watch(mode="schedule", run_as="batch", interval="15m", source=source_dir)
        .collect([".pdf"])
        .step(summarize_batch)
        .run_once()[0]
        .current
    )

    assert result == ("claims_a.pdf", "claims_b.pdf")


def test_show_returns_single_current_value_and_is_disabled_in_compiled_context():
    flow = Flow(name="preview", group="Manual").step(lambda context: pl.DataFrame({"x": [1]}))

    frame = flow.show()
    assert isinstance(frame, pl.DataFrame)
    assert frame.height == 1

    with compiled_flow_module_context():
        with pytest.raises(FlowValidationError, match="not available inside compiled flow modules"):
            flow.show()

    with compiled_flow_module_context():
        with pytest.raises(FlowValidationError, match="not available inside compiled flow modules"):
            flow.preview()


def test_preview_returns_final_current_value():
    flow = (
        Flow(name="preview_final", group="Manual")
        .step(lambda context: pl.DataFrame({"x": [1, 2]}), save_as="raw_df")
        .step(lambda context: context.current.filter(pl.col("x") > 1), use="raw_df")
    )

    frame = flow.preview()

    assert isinstance(frame, pl.DataFrame)
    assert frame.to_dict(as_series=False) == {"x": [2]}


def test_preview_returns_named_saved_object():
    flow = (
        Flow(name="preview_saved", group="Manual")
        .step(lambda context: pl.DataFrame({"x": [1, 2]}), save_as="raw_df")
        .step(lambda context: context.current.filter(pl.col("x") > 1), use="raw_df", save_as="filtered_df")
    )

    frame = flow.preview(use="raw_df")

    assert isinstance(frame, pl.DataFrame)
    assert frame.to_dict(as_series=False) == {"x": [1, 2]}


def test_preview_short_circuits_after_named_saved_object(tmp_path):
    marker = tmp_path / "should_not_exist.txt"

    def write_marker(context):
        marker.write_text("written", encoding="utf-8")
        return marker

    flow = (
        Flow(name="preview_short_circuit", group="Manual")
        .step(lambda context: pl.DataFrame({"x": [1, 2]}), save_as="raw_df")
        .step(lambda context: context.current.filter(pl.col("x") > 1), use="raw_df", save_as="filtered_df")
        .step(write_marker, use="filtered_df", label="Write Marker")
    )

    frame = flow.preview(use="filtered_df")

    assert isinstance(frame, pl.DataFrame)
    assert frame.to_dict(as_series=False) == {"x": [2]}
    assert marker.exists() is False


def test_preview_requires_named_saved_object_when_requested():
    flow = Flow(name="preview_missing", group="Manual").step(lambda context: pl.DataFrame({"x": [1]}))

    with pytest.raises(FlowValidationError, match="could not find saved object 'missing_df'"):
        flow.preview(use="missing_df")


def test_preview_uses_first_deterministic_poll_source_when_directory_has_many_files(tmp_path):
    source_dir = tmp_path / "input"
    source_dir.mkdir()
    for name in ("b.xlsx", "a.xlsx", "c.xlsx"):
        (source_dir / name).write_text("placeholder", encoding="utf-8")

    flow = (
        Flow(name="preview_poll_many", group="Claims")
        .watch(mode="poll", source=source_dir, interval="5s", extensions=[".xlsx"])
        .step(lambda context: pl.DataFrame({"source_name": [context.source.path.name]}), save_as="raw_df")
    )

    frame = flow.preview(use="raw_df")

    assert isinstance(frame, pl.DataFrame)
    assert frame.to_dict(as_series=False) == {"source_name": ["a.xlsx"]}


def test_runtime_uses_saved_objects_and_collects_step_output_paths(tmp_path):
    source = tmp_path / "input.xlsx"
    source.write_text("placeholder", encoding="utf-8")
    target = tmp_path / "output" / "input.parquet"

    def read_source(context):
        return pl.DataFrame({"source": [context.source.path.name]})

    def write_target(context):
        output = context.mirror.with_suffix(".parquet")
        context.current.write_parquet(output)
        return output

    results = (
        Flow(name="claims_poll", group="Claims")
        .watch(mode="poll", source=source, interval="5s")
        .mirror(root=target.parent)
        .step(read_source, save_as="raw_df", label="Read Excel")
        .step(write_target, use="raw_df", label="Write Parquet")
        .run_once()
    )

    context = results[0]
    assert context.source is not None
    assert context.source.path == source.resolve()
    assert context.mirror is not None
    assert context.mirror.with_suffix(".parquet") == target.resolve()
    assert context.metadata["step_outputs"]["Write Parquet"] == target.resolve()
    assert context.metadata["file_hash"] == hashlib.sha1("input.xlsx".encode("utf-8")).hexdigest()


def test_runtime_metadata_file_hash_uses_source_relative_path_for_directory_sources(tmp_path):
    source_dir = tmp_path / "incoming"
    nested_dir = source_dir / "2026" / "04"
    nested_dir.mkdir(parents=True)
    source = nested_dir / "claims.xlsx"
    source.write_text("placeholder", encoding="utf-8")

    context = (
        Flow(name="claims_poll_dir", group="Claims")
        .watch(mode="poll", source=source_dir, interval="5s", extensions=[".xlsx"])
        .step(lambda current_context: current_context)
        .run_once()[0]
        .current
    )

    expected = hashlib.sha1("2026/04/claims.xlsx".encode("utf-8")).hexdigest()
    assert context.metadata["file_hash"] == expected


def test_schedule_exposes_bound_paths_in_context(tmp_path):
    source_file = tmp_path / "input.xlsx"
    source_file.write_text("placeholder", encoding="utf-8")

    def capture(context):
        return {
            "source_path": context.source.path,
            "source_root": context.source.root,
            "source_json": context.source.with_suffix(".json"),
            "mirror_path": context.mirror.with_suffix(".parquet"),
        }

    result = (
        Flow(name="scheduled_paths", group="Claims")
        .watch(mode="schedule", run_as="batch", interval="10m", source=source_file)
        .mirror(root=tmp_path)
        .step(capture)
        .run_once()[0]
        .current
    )

    assert result["source_path"] == source_file.resolve()
    assert result["source_root"] == tmp_path.resolve()
    assert result["source_json"] == (tmp_path / "input.json").resolve()


def test_schedule_missing_source_file_records_failed_run_and_log(tmp_path):
    ledger = RuntimeLedger.open_default(data_root=tmp_path)
    flow = (
        Flow(name="scheduled_missing_file", group="Claims")
        .watch(mode="schedule", run_as="batch", interval="10m", source=tmp_path / "missing.xlsx")
        .step(lambda context: {"ok": True})
    )

    runtime = _FlowRuntime((flow,), continuous=False, runtime_ledger=ledger)

    with pytest.raises(FlowValidationError, match="Source path not found"):
        runtime.run()

    runs = ledger.list_runs(flow_name="scheduled_missing_file")
    logs = ledger.list_logs(flow_name="scheduled_missing_file")

    assert len(runs) == 1
    assert runs[0].status == "failed"
    assert "Source path not found" in str(runs[0].error_text)


def test_flow_context_config_supports_get_require_names_and_all(tmp_path):
    workspace_root = tmp_path / "workspace"
    config_dir = workspace_root / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "claims.toml").write_text(
        """
        [runtime]
        batch_size = 12000

        [input]
        pattern = "*.xlsx"
        """.strip()
        + "\n",
        encoding="utf-8",
    )
    flow = Flow(name="claims", group="Claims")._clone(_workspace_root=workspace_root)
    context = _FlowRuntime((flow.step(lambda current_context: current_context.config),), continuous=False).run()[0]
    cfg = context.current

    assert cfg.names() == ("claims",)
    assert cfg.get("missing") is None
    assert cfg.get("claims")["runtime"]["batch_size"] == 12000
    assert cfg.require("claims")["input"]["pattern"] == "*.xlsx"
    assert cfg.all()["claims"]["runtime"]["batch_size"] == 12000


def test_flow_context_config_require_raises_for_missing_config(tmp_path):
    workspace_root = tmp_path / "workspace"
    (workspace_root / "config").mkdir(parents=True)
    flow = Flow(name="claims", group="Claims")._clone(_workspace_root=workspace_root)

    with pytest.raises(FlowValidationError, match="Required config file was not found"):
        _FlowRuntime((flow.step(lambda context: context.config.require("claims")),), continuous=False).run()


def test_flow_context_database_returns_write_ready_workspace_database_path(tmp_path):
    workspace_root = tmp_path / "workspace"
    (workspace_root / "databases").mkdir(parents=True)
    flow = Flow(name="claims", group="Claims")._clone(_workspace_root=workspace_root)

    context = _FlowRuntime((flow.step(lambda current_context: current_context.database("claims/db.duckdb")),), continuous=False).run()[0]

    assert context.current == (workspace_root / "databases" / "claims" / "db.duckdb").resolve()
    assert context.current.parent.is_dir()


def test_flow_context_database_rejects_absolute_paths(tmp_path):
    workspace_root = tmp_path / "workspace"
    flow = Flow(name="claims", group="Claims")._clone(_workspace_root=workspace_root)

    with pytest.raises(FlowValidationError, match="name must be relative"):
        _FlowRuntime((flow.step(lambda context: context.database(tmp_path / "outside.duckdb")),), continuous=False).run()


def test_poll_missing_source_dir_records_failed_run_and_log(tmp_path):
    ledger = RuntimeLedger.open_default(data_root=tmp_path)
    flow = (
        Flow(name="poll_missing_dir", group="Claims")
        .watch(mode="poll", source=tmp_path / "missing_input", interval="5s")
        .step(lambda context: {"ok": True})
    )

    runtime = _FlowRuntime((flow,), continuous=False, runtime_ledger=ledger)

    with pytest.raises(FlowValidationError, match="Source path not found"):
        runtime.run()

    runs = ledger.list_runs(flow_name="poll_missing_dir")
    logs = ledger.list_logs(flow_name="poll_missing_dir")

    assert len(runs) == 1
    assert runs[0].status == "failed"
    assert "Source path not found" in str(runs[0].error_text)
    assert any("status=failed" in entry.message for entry in logs)


def test_batch_poll_marks_all_stale_source_files_success_in_ledger(tmp_path):
    ledger = RuntimeLedger.open_default(data_root=tmp_path)
    source_dir = tmp_path / "input"
    source_dir.mkdir()
    first = source_dir / "a.parquet"
    second = source_dir / "b.parquet"
    pl.DataFrame({"value": [1]}).write_parquet(first)
    pl.DataFrame({"value": [2]}).write_parquet(second)

    flow = (
        Flow(name="batch_poll", group="Claims")
        .watch(mode="poll", run_as="batch", source=source_dir, interval="5s", extensions=[".parquet"])
        .step(lambda context: {"root": context.source.root, "path": context.source.path})
    )

    runtime = _FlowRuntime((flow,), continuous=False, runtime_ledger=ledger)
    results = runtime.run()

    assert len(results) == 1
    assert results[0].current["root"] == source_dir.resolve()
    assert results[0].current["path"] is None

    states = {Path(item.source_path).name: item for item in ledger.list_file_states(flow_name="batch_poll")}
    assert set(states) == {"a.parquet", "b.parquet"}
    assert all(item.last_status == "success" for item in states.values())
    assert all(item.last_success_run_id is not None for item in states.values())

    assert runtime._stale_poll_sources(flow) == []


def test_runtime_uses_injected_ledger_factory_once(tmp_path):
    calls: list[str] = []

    def open_ledger():
        calls.append("called")
        return RuntimeLedger.open_default(data_root=tmp_path)

    flow = Flow(name="factory_runtime", group="Claims").step(lambda context: context.current)

    runtime = _FlowRuntime((flow,), continuous=False, runtime_ledger_factory=open_ledger)
    runtime.run()

    assert calls == ["called"]


def test_runtime_uses_injected_ledger_service_once(tmp_path):
    calls: list[str] = []

    class _Service:
        def open_runtime_ledger(self):
            calls.append("called")
            return RuntimeLedger.open_default(data_root=tmp_path)

    flow = Flow(name="service_runtime", group="Claims").step(lambda context: context.current)

    runtime = _FlowRuntime((flow,), continuous=False, runtime_ledger_service=_Service())
    runtime.run()

    assert calls == ["called"]


def test_grouped_runtime_uses_injected_ledger_factory_once(tmp_path):
    calls: list[str] = []

    def open_ledger():
        calls.append("called")
        return RuntimeLedger.open_default(data_root=tmp_path)

    grouped = _GroupedFlowRuntime(
        (
            Flow(name="grouped_factory_first", group="shared").step(lambda context: context.current),
            Flow(name="grouped_factory_second", group="shared").step(lambda context: context.current),
        ),
        continuous=False,
        runtime_ledger_factory=open_ledger,
    )

    grouped.run()

    assert calls == ["called"]


def test_grouped_runtime_uses_injected_ledger_service_once(tmp_path):
    calls: list[str] = []

    class _Service:
        def open_runtime_ledger(self):
            calls.append("called")
            return RuntimeLedger.open_default(data_root=tmp_path)

    grouped = _GroupedFlowRuntime(
        (
            Flow(name="grouped_service_first", group="shared").step(lambda context: context.current),
            Flow(name="grouped_service_second", group="shared").step(lambda context: context.current),
        ),
        continuous=False,
        runtime_ledger_service=_Service(),
    )

    grouped.run()

    assert calls == ["called"]


def test_flow_run_once_uses_injected_runtime_execution_service():
    calls: list[Flow] = []
    flow = Flow(name="service_runtime", group="Claims").step(lambda context: context.current)

    class _RuntimeExecutionService:
        def run_once(self, flow_arg):
            calls.append(flow_arg)
            return ["ok"]

    assert flow.run_once(runtime_execution_service=_RuntimeExecutionService()) == ["ok"]
    assert calls == [flow]


def test_load_flow_and_discover_flows_use_injected_flow_execution_service(tmp_path):
    loaded = Flow(name="loaded", group="Claims").step(lambda context: context.current)
    discovered = (loaded,)
    calls: list[tuple[str, Path | None] | tuple[str, Path | None]] = []

    class _FlowExecutionService:
        def load_flow(self, name, *, workspace_root=None):
            calls.append((name, workspace_root))
            return loaded

        def discover_flows(self, *, workspace_root=None):
            calls.append(("discover", workspace_root))
            return discovered

    from data_engine.authoring.flow import discover_flows, load_flow

    service = _FlowExecutionService()
    assert load_flow("loaded", data_root=tmp_path, flow_execution_service=service) is loaded
    assert discover_flows(data_root=tmp_path, flow_execution_service=service) == discovered
    assert calls == [("loaded", tmp_path), ("discover", tmp_path)]


def test_flow_public_entrypoints_accept_injected_services():
    flow = Flow(name="claims", group="Claims").step(lambda context: context.current)
    run_once_calls: list[Flow] = []
    preview_calls: list[tuple[Flow, str | None]] = []
    run_calls: list[tuple[Flow, ...]] = []
    load_calls: list[tuple[str, Path | None]] = []
    discover_calls: list[Path | None] = []

    class _RuntimeExecutionService:
        def run_once(self, flow_arg):
            run_once_calls.append(flow_arg)
            return ["once"]

        def preview(self, flow_arg, *, use=None):
            preview_calls.append((flow_arg, use))
            return "preview"

        def run_continuous(self, flow_arg):
            run_calls.append((flow_arg,))
            return ["continuous"]

        def run_grouped_continuous(self, flows_arg):
            run_calls.append(tuple(flows_arg))
            return ["grouped"]

    class _FlowExecutionService:
        def load_flow(self, name, *, workspace_root=None):
            load_calls.append((name, workspace_root))
            return flow

        def discover_flows(self, *, workspace_root=None):
            discover_calls.append(workspace_root)
            return (flow,)

    runtime_service = _RuntimeExecutionService()
    flow_service = _FlowExecutionService()

    assert flow.run_once(runtime_execution_service=runtime_service) == ["once"]
    assert flow.preview(runtime_execution_service=runtime_service) == "preview"
    assert flow.run(runtime_execution_service=runtime_service) == ["continuous"]
    assert load_flow("claims", data_root=Path("/tmp/workspace"), flow_execution_service=flow_service) is flow
    assert discover_flows(data_root=Path("/tmp/workspace"), flow_execution_service=flow_service) == (flow,)
    assert run(flow, runtime_execution_service=runtime_service) == ["grouped"]

    assert run_once_calls == [flow]
    assert preview_calls == [(flow, None)]
    assert run_calls == [(flow,), (flow,)]
    assert load_calls == [("claims", Path("/tmp/workspace"))]
    assert discover_calls == [Path("/tmp/workspace")]


def test_runtime_requires_all_flows_to_have_steps():
    with pytest.raises(FlowValidationError, match="must define at least one step"):
        _FlowRuntime((Flow(name="empty", group="Claims"),), continuous=False).run()


def test_runtime_requires_unique_flow_names():
    first = Flow(name="duplicate", group="Claims").step(lambda context: context.current)
    second = Flow(name="duplicate", group="Reports").step(lambda context: context.current)

    with pytest.raises(FlowValidationError, match="unique"):
        _FlowRuntime((first, second), continuous=False).run()


def test_runtime_raises_when_step_uses_missing_saved_object():
    flow = (
        Flow(name="missing_saved", group="Claims")
        .step(lambda context: "value")
        .step(lambda context: context.current, use="not_there")
    )

    with pytest.raises(FlowValidationError, match="missing object"):
        flow.run_once()


def test_runtime_stops_when_flow_stop_event_is_set():
    stop_event = threading.Event()
    stop_event.set()
    flow = Flow(name="stopped", group="Claims").step(lambda context: context.current)

    with pytest.raises(Exception, match="stop requested"):
        _FlowRuntime((flow,), continuous=False, flow_stop_event=stop_event).run()


def test_poll_rejects_negative_settle(tmp_path):
    source_dir = tmp_path / "input"
    source_dir.mkdir()

    with pytest.raises(FlowValidationError, match="greater than or equal to zero"):
        Flow(name="bad_settle", group="Claims").watch(
            mode="poll",
            source=source_dir,
            interval="5s",
            settle=-1,
        )


def test_step_requires_exactly_one_context_parameter():
    def no_args():
        return None

    def two_args(context, value):
        return value

    with pytest.raises(FlowValidationError, match="exactly one context parameter"):
        Flow(name="bad_arity_1", group="Claims").step(no_args)

    with pytest.raises(FlowValidationError, match="exactly one context parameter"):
        Flow(name="bad_arity_2", group="Claims").step(two_args)


def test_map_requires_iterable_current_and_one_item_parameter():
    def three_args(context, item, extra):
        return item

    with pytest.raises(FlowValidationError, match="either \\(item\\) or \\(context, item\\)"):
        Flow(name="bad_each_arity", group="Claims").map(three_args)

    flow = (
        Flow(name="bad_each_runtime", group="Claims")
        .step(lambda context: "not iterable")
        .map(lambda item: item)
    )

    with pytest.raises(FlowValidationError, match="requires an iterable current value"):
        flow.run_once()


def test_map_rejects_empty_batches(tmp_path):
    source_dir = tmp_path / "input"
    source_dir.mkdir()

    flow = (
        Flow(name="empty_map", group="Claims")
        .watch(mode="schedule", run_as="batch", interval="15m", source=source_dir)
        .collect([".pdf"])
        .map(lambda item: item, label="Read Pdf")
    )

    with pytest.raises(FlowValidationError, match='failed in step "Read Pdf".*map\\(\\) requires at least one item'):
        flow.run_once()


def test_grouped_runtime_keeps_order_within_group():
    order: list[str] = []

    def mark(label: str):
        def _inner(context):
            order.append(label)
            return context.current

        return _inner

    grouped = _GroupedFlowRuntime(
        (
            Flow(name="first", group="shared").step(mark("first")),
            Flow(name="second", group="shared").step(mark("second")),
        ),
        continuous=False,
        runtime_stop_event=threading.Event(),
    )

    grouped.run()

    assert order == ["first", "second"]
