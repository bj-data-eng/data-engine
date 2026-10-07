"""Captured workspace context and resource ownership for background GUI jobs."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING

from data_engine.ui.gui.helpers import start_worker_thread

if TYPE_CHECKING:
    from pathlib import Path

    from data_engine.platform.workspace_models import WorkspacePaths
    from data_engine.services.runtime_binding import WorkspaceRuntimeBinding
    from data_engine.ui.gui.app import DataEngineWindow


class _BindingJobs:
    def __init__(self, service) -> None:
        self.service = service
        self.lock = Lock()
        self.active: dict[int, tuple[object, int, bool]] = {}

    def retain(self, binding) -> None:
        with self.lock:
            _, count, retired = self.active.get(id(binding), (binding, 0, False))
            self.active[id(binding)] = (binding, count + 1, retired)

    def release(self, binding) -> None:
        with self.lock:
            _, count, retired = self.active[id(binding)]
            if count > 1:
                self.active[id(binding)] = (binding, count - 1, retired)
            else:
                del self.active[id(binding)]
                if retired:
                    self.service.close_binding(binding)

    def retire(self, binding) -> None:
        with self.lock:
            entry = self.active.get(id(binding))
            if entry is None:
                self.service.close_binding(binding)
            else:
                self.active[id(binding)] = (binding, entry[1], True)


@dataclass(frozen=True)
class WorkspaceJob:
    """Dispatch-time identity and retained binding for one background job."""

    paths: WorkspacePaths
    binding: WorkspaceRuntimeBinding
    token: tuple[int, str]
    timing_log_path: Path | None
    owners: _BindingJobs


def _binding_jobs(window: DataEngineWindow) -> _BindingJobs:
    owners = getattr(window, "_binding_jobs", None)
    if owners is None:
        owners = _BindingJobs(window.runtime_binding_service)
        window._binding_jobs = owners
    return owners


def start_workspace_job(window: DataEngineWindow, *, target, args=()) -> None:
    """Capture the active workspace and retain its binding until the target exits."""
    owners = _binding_jobs(window)
    job = WorkspaceJob(
        paths=window.workspace_paths,
        binding=window.runtime_binding,
        token=window._workspace_binding_token(),
        timing_log_path=window._ui_timing_log_path,
        owners=owners,
    )
    owners.retain(job.binding)
    started = False

    def run() -> None:
        nonlocal started
        started = True
        try:
            target(window, job, *args)
        finally:
            owners.release(job.binding)

    try:
        start_worker_thread(window, target=run)
    except BaseException:
        if not started:
            owners.release(job.binding)
        raise


def retire_workspace_binding(window: DataEngineWindow, binding: WorkspaceRuntimeBinding) -> None:
    """Close a retired binding once all jobs that captured it have exited."""
    _binding_jobs(window).retire(binding)
