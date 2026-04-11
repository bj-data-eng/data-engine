"""Environment and daemon diagnostics for the CLI surface."""

from __future__ import annotations

import sys
from typing import Any, Callable

from data_engine.authoring.model import FlowValidationError
from data_engine.domain import ClassifiedProcessInfo, DoctorCheck, ProcessInfo, WorkspaceLeaseDiagnostic
from data_engine.domain.diagnostics import is_defunct_process_status
from data_engine.platform.paths import path_display
from data_engine.platform.processes import (
    ProcessInspectionError,
    collapse_windows_launcher_processes,
    list_processes,
)
from data_engine.platform.workspace_models import authored_workspace_is_available, machine_id_text


def doctor(*, settings: Any, paths: Any) -> int:
    checks: list[DoctorCheck] = []

    def add(status: str, message: str) -> None:
        checks.append(DoctorCheck(status=status, message=message))

    add("OK", f"app root: {path_display(settings.app_root)}")
    add("OK", f"python executable: {path_display(sys.executable)}")
    add("OK" if settings.settings_path.is_file() else "WARN", f"workspace settings: {path_display(settings.settings_path)}")
    add("OK" if settings.state_root.exists() else "WARN", f"state root: {path_display(settings.state_root)}")
    add("OK" if settings.runtime_root.exists() else "WARN", f"runtime root: {path_display(settings.runtime_root)}")
    if settings.workspace_collection_root is None:
        add("WARN", "workspace collection root: not configured")
    else:
        add("OK" if settings.workspace_collection_root.is_dir() else "WARN", f"workspace collection root: {path_display(settings.workspace_collection_root)}")
    if paths.workspace_configured:
        add("OK" if paths.workspace_root.is_dir() else "WARN", f"workspace root: {path_display(paths.workspace_root)}")
        add("OK" if paths.flow_modules_dir.is_dir() else "WARN", f"flow modules dir: {path_display(paths.flow_modules_dir)}")
        add("OK" if (paths.workspace_root / ".vscode" / "settings.json").is_file() else "WARN", f"VS Code settings: {path_display(paths.workspace_root / '.vscode' / 'settings.json')}")
        add("OK" if authored_workspace_is_available(paths) else "WARN", f"authored workspace ready: {path_display(paths.workspace_root)}")
    else:
        add("WARN", "workspace root: not configured")
        add("WARN", "flow modules dir: not configured")
        add("WARN", "VS Code settings: not configured")
        add("WARN", "authored workspace ready: workspace collection root not configured")
    add("OK" if paths.artifacts_dir.exists() else "WARN", f"artifacts dir: {path_display(paths.artifacts_dir)}")

    failures = 0
    for check in checks:
        print(f"[{check.status}] {check.message}")
        if check.status == "FAIL":
            failures += 1
    return 1 if failures else 0


def run_process_listing() -> list[ProcessInfo]:
    try:
        return list_processes()
    except ProcessInspectionError as exc:
        raise FlowValidationError("Unable to inspect the local process table.") from exc


def classify_process_kind(command: str) -> str | None:
    if "data_engine.hosts.daemon.app" in command:
        return "daemon"
    if "data_engine.ui.gui.launcher" in command:
        return "gui"
    if "data_engine.ui.tui.app" in command:
        return "tui"
    return None


def doctor_daemons(
    *,
    settings: Any,
    workspace_service: Any,
    process_rows: list[ProcessInfo] | None = None,
    process_listing_func: Callable[[], list[ProcessInfo]] = run_process_listing,
    classify_process_kind_func: Callable[[str], str | None] = classify_process_kind,
    read_lease_metadata_func: Callable[[Any], dict[str, Any] | None],
    lease_is_stale_func: Callable[[Any, float], bool],
    machine_id_text_func: Callable[[], str] = machine_id_text,
) -> int:
    rows = process_rows if process_rows is not None else process_listing_func()
    relevant = [
        ClassifiedProcessInfo(
            pid=row.pid,
            ppid=row.ppid,
            status=row.status,
            command=row.command,
            kind=kind,
        )
        for row in rows
        for kind in (classify_process_kind_func(str(row.command)),)
        if kind is not None
    ]
    relevant = collapse_windows_launcher_processes(relevant)
    daemons = [row for row in relevant if row.kind == "daemon"]
    surfaces = [row for row in relevant if row.kind in {"gui", "tui"}]
    defunct = [row for row in daemons if row.is_defunct]
    live_daemons = [row for row in daemons if row not in defunct]

    print("Data Engine Daemon Diagnostics")
    print("")
    print(f"Live daemons: {len(live_daemons)}")
    for row in live_daemons:
        orphaned = " orphaned" if row.is_orphaned else ""
        print(f"  daemon pid={row.pid} ppid={row.ppid} status={row.status}{orphaned}")

    print("")
    print(f"Defunct daemons: {len(defunct)}")
    for row in defunct:
        print(f"  defunct pid={row.pid} ppid={row.ppid} status={row.status}")

    print("")
    print(f"Related UI processes: {len(surfaces)}")
    for row in surfaces:
        print(f"  {row.kind} pid={row.pid} ppid={row.ppid} status={row.status}")

    print("")
    print("Workspace leases:")
    discovered = ()
    if settings.workspace_collection_root is not None:
        discovered = workspace_service.discover(
            app_root=settings.app_root,
            workspace_collection_root=settings.workspace_collection_root,
        )
    any_workspace = False
    for item in discovered:
        any_workspace = True
        paths = workspace_service.resolve_paths(
            workspace_id=item.workspace_id,
            workspace_root=item.workspace_root,
            workspace_collection_root=settings.workspace_collection_root,
        )
        metadata = read_lease_metadata_func(paths)
        if metadata is None:
            print(f"  {item.workspace_id}: no lease metadata")
            continue
        pid_value = metadata.get("pid")
        try:
            pid = int(pid_value)
        except (TypeError, ValueError):
            pid = None
        matching = next((row for row in rows if row.pid == pid), None) if pid is not None else None
        owner = metadata.get("machine_id")
        if matching is None:
            status = "missing"
        elif is_defunct_process_status(matching.status):
            status = "defunct"
        else:
            status = "live"
        lease_row = WorkspaceLeaseDiagnostic(
            workspace_id=item.workspace_id,
            lease_pid=pid,
            state=status,
            stale=lease_is_stale_func(paths, stale_after_seconds=30.0),
            local_owner=owner == machine_id_text_func(),
        )
        stale_text = " stale" if lease_row.stale else ""
        local_text = " local" if lease_row.local_owner else ""
        print(
            f"  {lease_row.workspace_id}: lease_pid={lease_row.lease_pid if lease_row.lease_pid is not None else '-'} "
            f"state={lease_row.state}{stale_text}{local_text}"
        )
    if not any_workspace:
        print("  no discovered workspaces")
    return 0
