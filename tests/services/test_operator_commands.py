from __future__ import annotations

from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.services.operator_commands import OperatorCommandService
from data_engine.services.runtime_io import RuntimeIoLayer

from tests.services.support import resolve_workspace_paths


def test_operator_command_service_normalizes_control_and_runtime_results(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")

    class _ControlApplication:
        def run_selected_flow(self, **kwargs):
            assert kwargs["paths"] == paths
            return type(
                "_Result",
                (),
                {
                    "requested": True,
                    "sync_after": True,
                    "ensure_daemon_started": False,
                    "status_text": "accepted",
                    "error_text": None,
                },
            )()

    class _RuntimeApplication:
        def force_shutdown_daemon(self, paths_arg, *, timeout=0.5):
            assert paths_arg == paths
            assert timeout == 0.5
            return type("_Result", (), {"ok": True, "error": None})()

    service = OperatorCommandService(
        control_application=_ControlApplication(),
        runtime_application=_RuntimeApplication(),
        reset_service=object(),
        workspace_provisioning_service=None,
    )

    run_result = service.run_selected_flow(paths=paths)
    stop_result = service.force_shutdown_daemon(paths, timeout=0.5)

    assert run_result.requested is True
    assert run_result.sync_after is True
    assert run_result.status_text == "accepted"
    assert stop_result.error_text is None


def test_operator_command_service_normalizes_reset_and_provision_errors(tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    runtime_cache_ledger = object()
    runtime_control_ledger = object()

    class _ResetService:
        def reset_workspace(self, **kwargs):
            del kwargs
            raise RuntimeError("reset boom")

    class _RuntimeApplication:
        def reset_flow(self, *args, **kwargs):
            del args, kwargs
            return type("Result", (), {"ok": False, "error": "flow boom"})()

    service = OperatorCommandService(
        control_application=object(),
        runtime_application=_RuntimeApplication(),
        reset_service=_ResetService(),
        workspace_provisioning_service=None,
    )

    workspace_reset = service.reset_workspace(
        paths=paths,
        runtime_cache_ledger=runtime_cache_ledger,
        runtime_control_ledger=runtime_control_ledger,
    )
    flow_reset = service.reset_flow(
        paths=paths,
        runtime_cache_ledger=runtime_cache_ledger,
        flow_name="docs_poll",
    )
    provision = service.provision_workspace(paths)

    assert workspace_reset.workspace_id == paths.workspace_id
    assert workspace_reset.error_text == "reset boom"
    assert flow_reset.flow_name == "docs_poll"
    assert flow_reset.error_text == "flow boom"
    assert provision.error_text == "Workspace provisioning is not available for this surface."


def test_successful_daemon_flow_reset_invalidates_cached_external_history(tmp_path, monkeypatch):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    db_path = paths.runtime_cache_db_path
    external_ledger = RuntimeCacheLedger(db_path)
    started = utcnow_text()
    external_ledger.runs.record_started(
        run_id="run-1",
        flow_name="docs_poll",
        group_name="Docs",
        source_path=None,
        started_at_utc=started,
    )
    runtime_cache_ledger = RuntimeIoLayer(cache_ttl_seconds=60.0).open_cache_store(db_path)
    monkeypatch.setattr(
        runtime_cache_ledger._handle,  # noqa: SLF001 - hold the external signature stable to exercise explicit invalidation
        "_sqlite_signature",
        lambda: ((True, 1, 1), (False, None, None)),
    )
    assert [run.run_id for run in runtime_cache_ledger.runs.list(flow_name="docs_poll")] == ["run-1"]

    class _RuntimeApplication:
        def reset_flow(self, paths_arg, *, name):
            assert paths_arg == paths
            external_ledger.reset_flow(name)
            return type("Result", (), {"ok": True, "error": None})()

    service = OperatorCommandService(
        control_application=object(),
        runtime_application=_RuntimeApplication(),
        reset_service=object(),
        workspace_provisioning_service=None,
    )
    try:
        result = service.reset_flow(
            paths=paths,
            runtime_cache_ledger=runtime_cache_ledger,
            flow_name="docs_poll",
        )

        assert result.error_text is None
        assert runtime_cache_ledger.runs.list(flow_name="docs_poll") == ()
    finally:
        runtime_cache_ledger.close()
        external_ledger.close()
