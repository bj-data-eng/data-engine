"""Exercise abrupt daemon death and restart against isolated runtime state."""

from __future__ import annotations

import time

from data_engine.hosts.daemon.client import (
    DaemonClientError,
    daemon_request,
    force_shutdown_daemon_process,
    is_daemon_live,
    local_daemon_has_exited,
    spawn_daemon_process,
)
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager
from data_engine.runtime.runtime_db import RuntimeCacheLedger

from .support import resolve_workspace_paths


def _wait_until(predicate, *, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("Daemon did not reach the expected state before the deadline")


def test_abrupt_daemon_exit_preserves_recent_work_and_restarts_with_fresh_lease(tmp_path):
    workspace = tmp_path / "workspace"
    flows = workspace / "flow_modules"
    flows.mkdir(parents=True)
    (flows / "crash.py").write_text(
        """from data_engine import Flow
import os
import sys

def crash(context):
    print('abrupt-exit sentinel', file=sys.stderr, flush=True)
    os._exit(23)

def build():
    return Flow(name='crash', group='Crash').step(crash, label='Crash')
""",
        encoding="utf-8",
    )
    paths = resolve_workspace_paths(workspace_root=workspace)
    ledger = RuntimeCacheLedger(paths.runtime_db_path)
    try:
        spawn_daemon_process(paths)
        _wait_until(lambda: is_daemon_live(paths))
        before = daemon_request(paths, {"command": "daemon_status"})["status"]["daemon_id"]
        try:
            response = daemon_request(paths, {"command": "run_flow", "name": "crash", "wait": False})
            assert response["ok"], response
        except DaemonClientError:
            # Abrupt exit can win the race with the command acknowledgement.
            pass
        _wait_until(lambda: local_daemon_has_exited(paths))
        snapshot = WorkspaceDaemonManager(paths).sync()
        assert snapshot.source == "exited"
        assert not snapshot.active_runs and not snapshot.runtime_active
        runs = ledger.runs.list()
        assert len(runs) == 1
        run_id = runs[0].run_id
        assert ledger.step_outputs.list()[0].run_id == run_id
        log = paths.runtime_state_dir / "daemon-crash.log"
        assert "abrupt-exit sentinel" in log.read_text()

        # The heartbeat is fresh: this must recover exact local death without
        # waiting for the normal remote-owner staleness interval.
        spawn_daemon_process(paths)
        _wait_until(lambda: is_daemon_live(paths))
        after = daemon_request(paths, {"command": "daemon_status"})["status"]["daemon_id"]
        assert after != before
        assert [(run.run_id, run.status) for run in ledger.runs.list()] == [(run_id, "stopped")]
        assert ledger.step_outputs.list()[0].status == "stopped"
        assert "abrupt-exit sentinel" in log.read_text()
    finally:
        force_shutdown_daemon_process(paths)
        ledger.close()
