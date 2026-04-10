"""Diagnostic state models shared across operator surfaces."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DoctorCheck:
    """One doctor check row with status and message."""

    status: str
    message: str


@dataclass(frozen=True)
class ProcessInfo:
    """One relevant local process row."""

    pid: int
    ppid: int
    status: str
    command: str


@dataclass(frozen=True)
class ClassifiedProcessInfo:
    """One local process row with Data Engine role classification."""

    pid: int
    ppid: int
    status: str
    command: str
    kind: str

    @property
    def is_defunct(self) -> bool:
        """Return whether this process row represents a zombie/defunct process."""
        return is_defunct_process_status(self.status)

    @property
    def is_orphaned(self) -> bool:
        """Return whether this process row is now parented by init/launchd."""
        if os.name == "nt":
            return False
        return self.ppid == 1


def is_defunct_process_status(status: str) -> bool:
    """Return whether a process status text indicates a defunct process."""
    normalized = status.strip().lower()
    if normalized == "defunct":
        return True
    if os.name == "nt":
        return False
    return normalized.startswith("z")


@dataclass(frozen=True)
class WorkspaceLeaseDiagnostic:
    """One workspace lease health row for CLI diagnostics."""

    workspace_id: str
    lease_pid: int | None
    state: str
    stale: bool
    local_owner: bool

