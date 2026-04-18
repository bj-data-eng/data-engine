from __future__ import annotations

from data_engine.services.operator_commands import OperatorCommandService

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

        def reset_flow(self, **kwargs):
            del kwargs
            raise RuntimeError("flow boom")

    service = OperatorCommandService(
        control_application=object(),
        runtime_application=object(),
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
        flow_name="claims_poll",
    )
    provision = service.provision_workspace(paths)

    assert workspace_reset.workspace_id == paths.workspace_id
    assert workspace_reset.error_text == "reset boom"
    assert flow_reset.flow_name == "claims_poll"
    assert flow_reset.error_text == "flow boom"
    assert provision.error_text == "Workspace provisioning is not available for this surface."
