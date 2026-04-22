#!/usr/bin/env python3
"""Live multi-workspace smoke suite for Data Engine."""

from __future__ import annotations

# ruff: noqa: E402

import argparse
from dataclasses import dataclass
import os
from pathlib import Path
import platform
import re
import shutil
import sys
import tempfile
import threading
import time
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from data_engine.hosts.daemon.app import (
    CHECKPOINT_INTERVAL_SECONDS,
    DaemonClientError,
    daemon_request,
    is_daemon_live,
    spawn_daemon_process,
)
from data_engine.devtools.smoke_data import build_temp_smoke_environment
from data_engine.platform.workspace_models import (
    DATA_ENGINE_APP_ROOT_ENV_VAR,
    DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR,
    DATA_ENGINE_RUNTIME_CONTROL_DB_PATH_ENV_VAR,
    DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR,
    DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ID_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR,
)
from data_engine.platform.local_settings import DATA_ENGINE_STATE_ROOT_ENV_VAR
from data_engine.platform.processes import force_kill_process_tree, list_processes, process_is_running
from data_engine.platform.workspace_policy import AppStatePolicy, RuntimeLayoutPolicy, WorkspaceDiscoveryPolicy
from data_engine.views.models import qt_flow_cards_from_entries
from data_engine.runtime.shared_state import read_lease_metadata, recover_stale_workspace
from data_engine.services import FlowCatalogService


@dataclass(frozen=True)
class DaemonProcess:
    pid: int
    workspace_root: Path | None
    command: str


_APP_STATE_POLICY = AppStatePolicy()
_WORKSPACE_DISCOVERY_POLICY = WorkspaceDiscoveryPolicy(app_state_policy=_APP_STATE_POLICY)
_RUNTIME_LAYOUT_POLICY = RuntimeLayoutPolicy(app_state_policy=_APP_STATE_POLICY, discovery_policy=_WORKSPACE_DISCOVERY_POLICY)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workspace-id",
        dest="workspace_ids",
        action="append",
        help="Workspace id to exercise. Defaults to example_workspace and docs2 when present.",
    )
    parser.add_argument(
        "--run-once-flow",
        default="example_manual",
        help="Manual flow to run once in each workspace.",
    )
    parser.add_argument(
        "--engine-seconds",
        type=float,
        default=3.0,
        help="Seconds to let each engine run before stopping it.",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=15.0,
        help="Seconds to wait for daemon startup.",
    )
    parser.add_argument(
        "--shutdown-timeout",
        type=float,
        default=15.0,
        help="Seconds to wait for daemon shutdown.",
    )
    parser.add_argument(
        "--recover-stale",
        action="store_true",
        help="Attempt stale-lease recovery for each workspace before testing.",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep the generated temporary smoke environment for debugging.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    flow_catalog_service = FlowCatalogService()
    failures: list[str] = []
    temp_root = Path(tempfile.mkdtemp(prefix="data_engine_live_suite_"))
    temp_app_root = temp_root / "app_root"
    temp_collection_root = temp_root / "workspaces"
    temp_state_root = temp_root / "app_local" / "data_engine"
    previous_app_root = os.environ.get(DATA_ENGINE_APP_ROOT_ENV_VAR)
    previous_collection_root = os.environ.get(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR)
    previous_workspace_root = os.environ.get(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR)
    previous_workspace_id = os.environ.get(DATA_ENGINE_WORKSPACE_ID_ENV_VAR)
    previous_runtime_cache_db = os.environ.get(DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR)
    previous_runtime_control_db = os.environ.get(DATA_ENGINE_RUNTIME_CONTROL_DB_PATH_ENV_VAR)
    previous_runtime_db = os.environ.get(DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR)
    previous_state_root = os.environ.get(DATA_ENGINE_STATE_ROOT_ENV_VAR)
    workspace_paths: list[Any] = []
    workspace_ids: list[str] = []
    try:
        workspace_ids = args.workspace_ids or ["example_workspace", "docs2"]

        build_temp_smoke_environment(temp_root=temp_root, workspace_ids=workspace_ids)
        os.environ[DATA_ENGINE_APP_ROOT_ENV_VAR] = str(temp_app_root)
        os.environ[DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR] = str(temp_collection_root)
        os.environ[DATA_ENGINE_STATE_ROOT_ENV_VAR] = str(temp_state_root)
        os.environ.pop(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, None)
        os.environ.pop(DATA_ENGINE_WORKSPACE_ID_ENV_VAR, None)
        os.environ.pop(DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR, None)
        os.environ.pop(DATA_ENGINE_RUNTIME_CONTROL_DB_PATH_ENV_VAR, None)
        os.environ.pop(DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR, None)

        settings = _APP_STATE_POLICY.load_settings(app_root=temp_app_root)
        discovered = _WORKSPACE_DISCOVERY_POLICY.discover(app_root=temp_app_root)
        discovered_ids = [item.workspace_id for item in discovered]

        print_section("Environment")
        print_kv("platform", platform.platform())
        print_kv("python", sys.version.replace("\n", " "))
        print_kv("project_root", PROJECT_ROOT)
        print_kv("temp_root", temp_root)
        print_kv("workspace_collection_root", settings.workspace_collection_root)
        print_kv("default_selected", settings.default_selected)
        print_kv("discovered_workspaces", ", ".join(discovered_ids) or "(none)")
        print_kv("target_workspaces", ", ".join(workspace_ids) or "(none)")

        workspace_paths = [_RUNTIME_LAYOUT_POLICY.resolve_paths(workspace_id=wid) for wid in workspace_ids]

        print_section("Preflight")
        for paths in workspace_paths:
            cards = qt_flow_cards_from_entries(flow_catalog_service.load_entries(workspace_root=paths.workspace_root))
            valid_cards = [card for card in cards if card.valid]
            manual_names = [card.name for card in valid_cards if card.mode == "manual"]
            flow_names = {card.name for card in valid_cards}
            notebook_names = {
                f"{paths.workspace_id}_nb_poll",
                f"{paths.workspace_id}_nb_schedule",
                f"{paths.workspace_id}_nb_manual",
            }
            record_check(paths.workspace_root.exists(), f"{paths.workspace_id}: workspace root exists", failures)
            record_check(paths.flow_modules_dir.exists(), f"{paths.workspace_id}: flow_modules exists", failures)
            record_check(args.run_once_flow in manual_names, f"{paths.workspace_id}: manual flow {args.run_once_flow} exists", failures)
            record_check(notebook_names <= flow_names, f"{paths.workspace_id}: notebook flows discovered", failures)
            print_kv(f"{paths.workspace_id}.workspace_root", paths.workspace_root)
            print_kv(f"{paths.workspace_id}.runtime_db_path", paths.runtime_db_path)
            print_kv(f"{paths.workspace_id}.cache_dir", paths.workspace_cache_dir)
            print_kv(f"{paths.workspace_id}.manual_flows", ", ".join(manual_names) or "(none)")
            for notebook_name in sorted(notebook_names):
                compiled_path = paths.compiled_flow_modules_dir / f"{notebook_name}.py"
                record_check(compiled_path.exists(), f"{paths.workspace_id}: compiled notebook module {notebook_name}.py exists", failures)

        if len({str(paths.runtime_db_path) for paths in workspace_paths}) != len(workspace_paths):
            failures.append("Runtime DB paths are not isolated per workspace.")
        if len({str(paths.compiled_flow_modules_dir) for paths in workspace_paths}) != len(workspace_paths):
            failures.append("Workspace cache paths are not isolated per workspace.")

        if args.recover_stale:
            print_section("Recover Stale")
            for paths in workspace_paths:
                recovered = recover_stale_workspace(
                    paths,
                    machine_id=platform.node() or "unknown-machine",
                    stale_after_seconds=CHECKPOINT_INTERVAL_SECONDS * 3,
                    reclaim=False,
                )
                print_kv(f"{paths.workspace_id}.recover_stale", recovered)

        initial_daemon_pids: dict[str, int | None] = {}

        print_section("Start Daemons")
        for paths in workspace_paths:
            try:
                spawn_daemon_process(paths)
            except Exception as exc:
                failures.append(f"{paths.workspace_id}: daemon start failed: {exc}")
                continue
            live = wait_for_daemon_start(paths, timeout=args.startup_timeout)
            record_check(live, f"{paths.workspace_id}: daemon became live", failures)
            if live:
                status = daemon_request(paths, {"command": "daemon_status"}, timeout=5.0).get("status", {})
                initial_pid = int(status.get("pid")) if status.get("pid") is not None else None
                initial_daemon_pids[paths.workspace_id] = initial_pid
                print_kv(f"{paths.workspace_id}.daemon_pid", status.get("pid"))
                print_kv(f"{paths.workspace_id}.daemon_status", status.get("status"))

        print_section("Duplicate Guard")
        for paths in workspace_paths:
            threads = [threading.Thread(target=_spawn_ignore_errors, args=(paths,)) for _ in range(3)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        time.sleep(1.0)
        processes = list_daemon_processes()
        for paths in workspace_paths:
            try:
                status = daemon_request(paths, {"command": "daemon_status"}, timeout=5.0).get("status", {})
                reported_pid = int(status.get("pid")) if status.get("pid") is not None else None
            except Exception:
                reported_pid = None
            matching = [item for item in processes if item.workspace_root == paths.workspace_root.resolve()]
            print_kv(f"{paths.workspace_id}.daemon_process_count", len(matching))
            print_kv(f"{paths.workspace_id}.daemon_pids", ", ".join(str(item.pid) for item in matching) or "(none)")
            initial_pid = initial_daemon_pids.get(paths.workspace_id)
            record_check(
                reported_pid is not None and initial_pid is not None and reported_pid == initial_pid,
                f"{paths.workspace_id}: exactly one daemon process",
                failures,
            )
            if reported_pid is not None:
                record_check(_is_pid_alive(reported_pid), f"{paths.workspace_id}: owner daemon pid is alive", failures)

        print_section("Run Once")
        for paths in workspace_paths:
            try:
                response = daemon_request(
                    paths,
                    {"command": "run_flow", "name": args.run_once_flow, "wait": True},
                    timeout=120.0,
                )
            except Exception as exc:
                failures.append(f"{paths.workspace_id}: run-once failed: {exc}")
                continue
            record_check(bool(response.get("ok")), f"{paths.workspace_id}: run-once succeeded", failures)

        print_section("Engine")
        for paths in workspace_paths:
            try:
                response = daemon_request(paths, {"command": "start_engine"}, timeout=10.0)
            except Exception as exc:
                failures.append(f"{paths.workspace_id}: start_engine failed: {exc}")
                continue
            record_check(bool(response.get("ok")), f"{paths.workspace_id}: engine start acknowledged", failures)
            active = wait_for_engine_state(paths, active=True, timeout=10.0)
            record_check(active, f"{paths.workspace_id}: engine became active", failures)
            time.sleep(max(args.engine_seconds, 0.0))
            try:
                response = daemon_request(paths, {"command": "stop_engine"}, timeout=10.0)
            except Exception as exc:
                failures.append(f"{paths.workspace_id}: stop_engine failed: {exc}")
                continue
            record_check(bool(response.get("ok")), f"{paths.workspace_id}: engine stop acknowledged", failures)
            stopped = wait_for_engine_state(paths, active=False, timeout=20.0)
            record_check(stopped, f"{paths.workspace_id}: engine stopped", failures)

        print_section("Lease State")
        daemon_pids: dict[str, int] = {}
        for paths in workspace_paths:
            metadata = read_lease_metadata(paths)
            print_kv(f"{paths.workspace_id}.lease", metadata or "(none)")
            record_check(metadata is not None, f"{paths.workspace_id}: lease metadata exists while daemon is running", failures)
            if metadata is not None:
                try:
                    daemon_pids[paths.workspace_id] = int(metadata.get("pid"))
                except (TypeError, ValueError):
                    failures.append(f"{paths.workspace_id}: lease metadata pid is invalid.")
            record_check((paths.leased_markers_dir / paths.workspace_id).exists(), f"{paths.workspace_id}: leased marker exists", failures)
            record_check(not (paths.available_markers_dir / paths.workspace_id).exists(), f"{paths.workspace_id}: available marker hidden while leased", failures)

        if len(set(daemon_pids.values())) != len(daemon_pids):
            failures.append("Different workspaces reused the same daemon pid.")

        print_section("Shutdown")
        for paths in workspace_paths:
            try:
                response = daemon_request(paths, {"command": "shutdown_daemon"}, timeout=10.0)
                record_check(bool(response.get("ok")), f"{paths.workspace_id}: shutdown acknowledged", failures)
            except Exception as exc:
                failures.append(f"{paths.workspace_id}: shutdown request failed: {exc}")
            stopped = wait_for_daemon_stop(paths, timeout=args.shutdown_timeout)
            record_check(stopped, f"{paths.workspace_id}: daemon stopped", failures)

        time.sleep(1.0)
        processes = list_daemon_processes()
        for paths in workspace_paths:
            matching = [item for item in processes if item.workspace_root == paths.workspace_root.resolve()]
            print_kv(f"{paths.workspace_id}.post_shutdown_process_count", len(matching))
            record_check(len(matching) == 0, f"{paths.workspace_id}: no daemon processes remain after shutdown", failures)
            old_pid = daemon_pids.get(paths.workspace_id)
            if old_pid is not None:
                print_kv(f"{paths.workspace_id}.post_shutdown_old_pid_alive", _is_pid_alive(old_pid))
                record_check(
                    not _is_pid_alive(old_pid),
                    f"{paths.workspace_id}: no orphaned daemon pid remains after shutdown",
                    failures,
                )
            record_check(read_lease_metadata(paths) is None, f"{paths.workspace_id}: lease metadata removed after shutdown", failures)
            record_check((paths.available_markers_dir / paths.workspace_id).exists(), f"{paths.workspace_id}: available marker restored after shutdown", failures)
            record_check(not (paths.leased_markers_dir / paths.workspace_id).exists(), f"{paths.workspace_id}: leased marker removed after shutdown", failures)
            record_check(paths.shared_runs_path.exists(), f"{paths.workspace_id}: shared runs parquet exists after shutdown", failures)
            record_check(paths.shared_step_runs_path.exists(), f"{paths.workspace_id}: shared step_runs parquet exists after shutdown", failures)
            record_check(paths.shared_logs_path.exists(), f"{paths.workspace_id}: shared logs parquet exists after shutdown", failures)
    finally:
        cleanup_temp_suite(workspace_paths)
        if previous_app_root is None:
            os.environ.pop(DATA_ENGINE_APP_ROOT_ENV_VAR, None)
        else:
            os.environ[DATA_ENGINE_APP_ROOT_ENV_VAR] = previous_app_root
        if previous_collection_root is None:
            os.environ.pop(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR, None)
        else:
            os.environ[DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR] = previous_collection_root
        if previous_workspace_root is None:
            os.environ.pop(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, None)
        else:
            os.environ[DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR] = previous_workspace_root
        if previous_workspace_id is None:
            os.environ.pop(DATA_ENGINE_WORKSPACE_ID_ENV_VAR, None)
        else:
            os.environ[DATA_ENGINE_WORKSPACE_ID_ENV_VAR] = previous_workspace_id
        if previous_runtime_cache_db is None:
            os.environ.pop(DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR, None)
        else:
            os.environ[DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR] = previous_runtime_cache_db
        if previous_runtime_control_db is None:
            os.environ.pop(DATA_ENGINE_RUNTIME_CONTROL_DB_PATH_ENV_VAR, None)
        else:
            os.environ[DATA_ENGINE_RUNTIME_CONTROL_DB_PATH_ENV_VAR] = previous_runtime_control_db
        if previous_runtime_db is None:
            os.environ.pop(DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR, None)
        else:
            os.environ[DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR] = previous_runtime_db
        if previous_state_root is None:
            os.environ.pop(DATA_ENGINE_STATE_ROOT_ENV_VAR, None)
        else:
            os.environ[DATA_ENGINE_STATE_ROOT_ENV_VAR] = previous_state_root
        if args.keep_temp:
            print_section("Temp Environment")
            print_kv("kept_temp_root", temp_root)
        else:
            shutil.rmtree(temp_root, ignore_errors=True)

    print_section("Result")
    if failures:
        print_kv("suite_status", "FAILED")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print_kv("suite_status", "PASSED")
    return 0


def _spawn_ignore_errors(paths) -> None:
    try:
        spawn_daemon_process(paths)
    except Exception:
        pass


def cleanup_temp_suite(workspace_paths: list[Any]) -> None:
    for paths in workspace_paths:
        try:
            daemon_request(paths, {"command": "shutdown_daemon"}, timeout=2.0)
        except Exception:
            pass
        try:
            force_kill_workspace_daemons(paths.workspace_root)
        except Exception:
            pass


def wait_for_daemon_start(paths, *, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if is_daemon_live(paths):
            return True
        time.sleep(0.1)
    return is_daemon_live(paths)


def wait_for_daemon_stop(paths, *, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not is_daemon_live(paths):
            return True
        time.sleep(0.1)
    return not is_daemon_live(paths)


def wait_for_engine_state(paths, *, active: bool, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = daemon_request(paths, {"command": "daemon_status"}, timeout=2.0)
        except DaemonClientError:
            time.sleep(0.2)
            continue
        status = response.get("status") if response.get("ok") else None
        if isinstance(status, dict) and bool(status.get("engine_active")) is active:
            return True
        time.sleep(0.2)
    return False


def list_daemon_processes() -> list[DaemonProcess]:
    processes: list[DaemonProcess] = []
    for row in list_processes():
        if "data_engine.hosts.daemon.app" not in row.command:
            continue
        workspace_root = _extract_workspace_root_from_command(row.command)
        processes.append(DaemonProcess(pid=row.pid, workspace_root=workspace_root, command=row.command))
    return processes


def force_kill_workspace_daemons(workspace_root: Path) -> None:
    for process in list_daemon_processes():
        if process.workspace_root != workspace_root.resolve():
            continue
        try:
            force_kill_process_tree(process.pid)
        except Exception:
            pass


def _is_pid_alive(pid: int) -> bool:
    return process_is_running(pid, treat_defunct_as_dead=False)


def _extract_workspace_root_from_command(command: str) -> Path | None:
    match = re.search(r'--workspace\s+(?:"([^"]+)"|(\S+))', command)
    if match is None:
        return None
    raw_path = match.group(1) or match.group(2)
    if not raw_path:
        return None
    return Path(raw_path).expanduser().resolve()


def record_check(condition: bool, label: str, failures: list[str]) -> None:
    print_kv(label, "PASS" if condition else "FAIL")
    if not condition:
        failures.append(label)


def print_section(title: str) -> None:
    print()
    print(f"=== {title} ===")


def print_kv(key: str, value: Any) -> None:
    print(f"{key}: {value}")


def test_live_runtime_suite() -> None:
    assert main(["--recover-stale"]) == 0


if __name__ == "__main__":
    raise SystemExit(main())
