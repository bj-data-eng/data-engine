from __future__ import annotations

import pytest

from tests.tui.support import FakeResetService, RecordingStatusTui, command_service_for_test, make_tui


@pytest.mark.anyio
async def test_tui_reset_flow_calls_persistent_reset_path():
    reset_service = FakeResetService()
    app = make_tui(command_service=command_service_for_test(reset_service=reset_service), app_cls=RecordingStatusTui)
    async with app.run_test():
        app.action_clear_flow_log()

        assert reset_service.flow_resets == [(app.workspace_paths, "poller")]
        assert app.status_messages[-1] == "Reset flow history for poller."

