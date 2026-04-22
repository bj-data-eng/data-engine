from __future__ import annotations

from pathlib import Path

from data_engine.platform.instrumentation import (
    DATA_ENGINE_DEV_INSTRUMENT_ENV_VAR,
    DATA_ENGINE_DEV_VIZTRACE_ENV_VAR,
    append_timing_line,
    dev_instrumentation_enabled,
    dev_viztrace_enabled,
    new_request_id,
    timed_operation,
)


def test_dev_instrumentation_is_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv(DATA_ENGINE_DEV_INSTRUMENT_ENV_VAR, raising=False)
    assert dev_instrumentation_enabled() is False


def test_dev_instrumentation_reads_truthy_env(monkeypatch) -> None:
    monkeypatch.setenv(DATA_ENGINE_DEV_INSTRUMENT_ENV_VAR, "1")
    assert dev_instrumentation_enabled() is True


def test_dev_viztrace_reads_truthy_env(monkeypatch) -> None:
    monkeypatch.setenv(DATA_ENGINE_DEV_VIZTRACE_ENV_VAR, "true")
    assert dev_viztrace_enabled() is True


def test_append_timing_line_writes_only_when_enabled(tmp_path: Path, monkeypatch) -> None:
    log_path = tmp_path / "timing.log"
    monkeypatch.delenv(DATA_ENGINE_DEV_INSTRUMENT_ENV_VAR, raising=False)
    append_timing_line(log_path, scope="gui.sync", event="noop")
    assert log_path.exists() is False

    monkeypatch.setenv(DATA_ENGINE_DEV_INSTRUMENT_ENV_VAR, "1")
    append_timing_line(
        log_path,
        scope="gui.sync",
        event="reload",
        fields={"workspace": "docs2", "request_id": "req-123"},
    )
    contents = log_path.read_text(encoding="utf-8")
    assert "scope=gui.sync" in contents
    assert "event=reload" in contents
    assert "workspace=docs2" in contents
    assert "request_id=req-123" in contents


def test_timed_operation_writes_only_end_when_threshold_is_crossed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv(DATA_ENGINE_DEV_INSTRUMENT_ENV_VAR, "1")
    log_path = tmp_path / "timing.log"
    with timed_operation(
        log_path,
        scope="daemon.runtime",
        event="run_flow",
        fields={"flow": "docs"},
        threshold_ms=0.0,
    ):
        pass
    lines = log_path.read_text(encoding="utf-8").splitlines()
    assert any("phase=end" in line and "elapsed_ms=" in line for line in lines)
    assert any("flow=docs" in line for line in lines)


def test_timed_operation_skips_fast_operations_below_threshold(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv(DATA_ENGINE_DEV_INSTRUMENT_ENV_VAR, "1")
    log_path = tmp_path / "timing.log"
    with timed_operation(log_path, scope="daemon.runtime", event="run_flow", threshold_ms=1000.0):
        pass
    assert log_path.exists() is False


def test_new_request_id_uses_prefix() -> None:
    request_id = new_request_id("run")
    assert request_id.startswith("run-")
    assert len(request_id) > 4

