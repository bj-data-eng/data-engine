"""Domain models for operator runtime and control state."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import TYPE_CHECKING, Iterable, Mapping

from data_engine.domain.catalog import FlowCatalogLike
from data_engine.domain.time import parse_utc_text

CONTROL_CHECKPOINT_INTERVAL_SECONDS = 30.0
CONTROL_STALE_AFTER_SECONDS = 90.0

if TYPE_CHECKING:
    from data_engine.hosts.daemon.manager import WorkspaceDaemonSnapshot


class DaemonLifecyclePolicy(str, Enum):
    """Lifecycle ownership policy for one daemon instance."""

    EPHEMERAL = "ephemeral"
    PERSISTENT = "persistent"

    @classmethod
    def coerce(cls, value: object) -> "DaemonLifecyclePolicy":
        """Normalize a raw lifecycle-policy input."""
        if isinstance(value, cls):
            return value
        text = str(value or "").strip().lower()
        if not text:
            return cls.PERSISTENT
        return cls(text)


@dataclass(frozen=True)
class ManualRunState:
    """One active manual run grouped by the owning flow group."""

    group_name: str | None
    flow_name: str


@dataclass(frozen=True)
class ActiveRunState:
    """One active run snapshot sourced from daemon or runtime state."""

    run_id: str
    flow_name: str
    group_name: str
    source_path: str | None
    state: str
    current_step_name: str | None = None
    current_step_started_at_utc: str | None = None
    started_at_utc: str | None = None
    finished_at_utc: str | None = None
    elapsed_seconds: float | None = None
    error_text: str | None = None


@dataclass(frozen=True)
class FlowActivityState:
    """One daemon-native flow activity summary."""

    flow_name: str
    active_run_count: int = 0
    queued_run_count: int = 0
    engine_run_count: int = 0
    manual_run_count: int = 0
    stopping_run_count: int = 0
    running_step_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeSessionState:
    """Runtime/control state shared by operator surfaces."""

    workspace_owned: bool = True
    leased_by_machine_id: str | None = None
    runtime_active: bool = False
    runtime_stopping: bool = False
    active_runtime_flow_names: tuple[str, ...] = ()
    manual_runs: tuple[ManualRunState, ...] = ()

    @classmethod
    def empty(cls) -> "RuntimeSessionState":
        """Return the default idle local-control runtime state."""
        return cls()

    @classmethod
    def from_daemon_snapshot(
        cls,
        snapshot: "WorkspaceDaemonSnapshot",
        flow_cards: Iterable[FlowCatalogLike],
    ) -> "RuntimeSessionState":
        """Build one normalized session state from a daemon snapshot and loaded flows."""
        cards = tuple(flow_cards)
        cards_by_name = {card.name: card for card in cards}
        manual_runs = tuple(
            ManualRunState(group_name=card.group, flow_name=flow_name)
            for flow_name in snapshot.manual_runs
            if (card := cards_by_name.get(flow_name)) is not None
        )
        active_runtime_flow_names: tuple[str, ...]
        if snapshot.runtime_active:
            active_runtime_flow_names = tuple(
                flow_name
                for flow_name in snapshot.active_engine_flow_names
                if (card := cards_by_name.get(flow_name)) is not None and card.valid and card.mode in {"poll", "schedule"}
            )
        else:
            active_runtime_flow_names = ()
        return cls(
            workspace_owned=snapshot.workspace_owned,
            leased_by_machine_id=snapshot.leased_by_machine_id,
            runtime_active=snapshot.runtime_active,
            runtime_stopping=snapshot.runtime_stopping,
            active_runtime_flow_names=active_runtime_flow_names,
            manual_runs=manual_runs,
        )

    @property
    def control_available(self) -> bool:
        """Return whether the current workstation may issue control actions."""
        return self.workspace_owned or self.leased_by_machine_id is None

    @property
    def manual_run_active(self) -> bool:
        """Return whether any manual runs are currently active."""
        return bool(self.manual_runs)

    @property
    def has_active_work(self) -> bool:
        """Return whether engine or manual work is currently active."""
        return self.runtime_active or self.manual_run_active

    @property
    def active_manual_runs(self) -> dict[str | None, str]:
        """Return active manual runs keyed by flow group."""
        return {run.group_name: run.flow_name for run in self.manual_runs}

    def manual_flow_name_for_group(self, group_name: str | None) -> str | None:
        """Return the active flow name for one group, if any."""
        for run in self.manual_runs:
            if run.group_name == group_name:
                return run.flow_name
        return None

    def is_group_active(self, group_name: str, flow_groups_by_name: Mapping[str, str]) -> bool:
        """Return whether a flow group is active through a manual run or engine run."""
        if self.manual_flow_name_for_group(group_name) is not None:
            return True
        if not self.runtime_active:
            return False
        return any(flow_groups_by_name.get(flow_name) == group_name for flow_name in self.active_runtime_flow_names)

    def with_manual_runs_map(self, active_manual_runs: Mapping[str | None, str]) -> "RuntimeSessionState":
        """Return a copy with active manual runs replaced from a mapping."""
        return replace(
            self,
            manual_runs=tuple(
                ManualRunState(group_name=group_name, flow_name=flow_name)
                for group_name, flow_name in active_manual_runs.items()
            ),
        )

    def without_manual_group(self, group_name: str | None) -> "RuntimeSessionState":
        """Return a copy with one manual-run group removed."""
        return replace(
            self,
            manual_runs=tuple(run for run in self.manual_runs if run.group_name != group_name),
        )

    def with_active_runtime_flow_names(self, flow_names: Iterable[str]) -> "RuntimeSessionState":
        """Return a copy with active engine-owned flow names replaced."""
        return replace(self, active_runtime_flow_names=tuple(flow_names))

    def with_runtime_flags(self, *, active: bool, stopping: bool) -> "RuntimeSessionState":
        """Return a copy with updated engine active/stopping flags."""
        return replace(self, runtime_active=active, runtime_stopping=stopping)

    def reset(self) -> "RuntimeSessionState":
        """Return the default idle state for a fresh workspace binding."""
        return type(self).empty()


@dataclass(frozen=True)
class DaemonStatusState:
    """Last normalized daemon status for one operator surface."""

    workspace_owned: bool = True
    leased_by_machine_id: str | None = None
    engine_active: bool = False
    engine_stopping: bool = False
    engine_starting: bool = False
    active_engine_flow_names: tuple[str, ...] = ()
    active_runs: tuple[ActiveRunState, ...] = ()
    flow_activity: tuple[FlowActivityState, ...] = ()
    manual_run_names: tuple[str, ...] = ()
    last_checkpoint_at_utc: str | None = None
    source: str = "none"
    transport_mode: str = "heartbeat"
    daemon_id: str | None = None
    projection_version: int = 0

    @classmethod
    def empty(cls) -> "DaemonStatusState":
        """Return the default no-daemon/no-lease status."""
        return cls()

    @classmethod
    def from_snapshot(cls, snapshot: "WorkspaceDaemonSnapshot") -> "DaemonStatusState":
        """Build one daemon-status value object from a daemon snapshot."""
        return cls(
            workspace_owned=snapshot.workspace_owned,
            leased_by_machine_id=snapshot.leased_by_machine_id,
            engine_active=snapshot.runtime_active,
            engine_stopping=snapshot.runtime_stopping,
            engine_starting=snapshot.engine_starting,
            active_engine_flow_names=snapshot.active_engine_flow_names,
            active_runs=snapshot.active_runs,
            flow_activity=snapshot.flow_activity,
            manual_run_names=tuple(snapshot.manual_runs),
            last_checkpoint_at_utc=snapshot.last_checkpoint_at_utc,
            source=snapshot.source,
            transport_mode=snapshot.transport_mode,
            daemon_id=snapshot.daemon_id,
            projection_version=snapshot.projection_version,
        )

    def as_runtime_session(self, flow_cards: Iterable[FlowCatalogLike]) -> RuntimeSessionState:
        """Project daemon status into runtime session state using the current loaded flows."""
        return RuntimeSessionState.from_daemon_snapshot(
            type("SnapshotProxy", (), {
                "workspace_owned": self.workspace_owned,
                "leased_by_machine_id": self.leased_by_machine_id,
                "runtime_active": self.engine_active,
                "runtime_stopping": self.engine_stopping,
                "active_engine_flow_names": self.active_engine_flow_names,
                "manual_runs": self.manual_run_names,
            })(),
            flow_cards,
        )

@dataclass(frozen=True)
class WorkspaceControlState:
    """Workspace lease/control state derived from one daemon snapshot."""

    daemon_status: DaemonStatusState
    control_status_text: str | None
    blocked_status_text: str
    local_request_pending: bool = False
    takeover_remaining_seconds: int | None = None

    @classmethod
    def empty(cls) -> "WorkspaceControlState":
        """Return the default no-daemon/no-lease control state."""
        return cls(
            daemon_status=DaemonStatusState.empty(),
            control_status_text=None,
            blocked_status_text="Takeover available.",
            local_request_pending=False,
            takeover_remaining_seconds=None,
        )

    @classmethod
    def from_snapshot(
        cls,
        snapshot: "WorkspaceDaemonSnapshot",
        *,
        daemon_live: bool,
        local_machine_id: str,
        control_request: Mapping[str, object] | None = None,
        daemon_startup_in_progress: bool = False,
        now_utc: datetime | None = None,
    ) -> "WorkspaceControlState":
        """Build one workspace control state from the latest daemon snapshot."""
        status = DaemonStatusState.from_snapshot(snapshot)
        local_request_pending = (
            isinstance(control_request, Mapping)
            and str(control_request.get("requester_machine_id", "")).strip() == local_machine_id
        )
        checkpoint_at = parse_utc_text(status.last_checkpoint_at_utc) if status.last_checkpoint_at_utc else None
        now = now_utc or datetime.now(UTC)

        control_status_text: str | None
        takeover_remaining_seconds: int | None = None

        if status.source == "none":
            control_status_text = None
        elif status.workspace_owned:
            if daemon_startup_in_progress:
                control_status_text = "Trying to restore local control..."
            elif checkpoint_at is None:
                control_status_text = "This Workstation has control"
            else:
                age = max((now - checkpoint_at).total_seconds(), 0.0)
                if age >= CONTROL_CHECKPOINT_INTERVAL_SECONDS and not daemon_live:
                    control_status_text = "Local engine is not responding"
                else:
                    control_status_text = "This Workstation has control"
        else:
            owner = status.leased_by_machine_id or "Another machine"
            if local_request_pending:
                control_status_text = f"Control requested from {owner}"
            elif checkpoint_at is None:
                control_status_text = f"{owner} has control"
            else:
                stale_at = checkpoint_at + timedelta(seconds=CONTROL_STALE_AFTER_SECONDS)
                takeover_remaining_seconds = max(int((stale_at - now).total_seconds()), 0)
                if takeover_remaining_seconds <= 0:
                    control_status_text = "Takeover available"
                else:
                    control_status_text = f"{owner} has control · takeover available in {takeover_remaining_seconds}s"

        if status.leased_by_machine_id is None:
            blocked_status_text = "Takeover available."
        else:
            blocked_status_text = f"{status.leased_by_machine_id} currently has control of this workspace."

        return cls(
            daemon_status=status,
            control_status_text=control_status_text,
            blocked_status_text=blocked_status_text,
            local_request_pending=local_request_pending,
            takeover_remaining_seconds=takeover_remaining_seconds,
        )
