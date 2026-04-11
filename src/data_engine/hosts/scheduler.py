"""APScheduler-backed host for scheduled flow execution."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Protocol

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from data_engine.core.primitives import WatchSpec
from data_engine.runtime.engine import RuntimeEngine

if TYPE_CHECKING:
    from data_engine.core.flow import Flow


class SchedulerPort(Protocol):
    """Small scheduler surface used by the scheduler host."""

    def add_job(self, func, *, trigger, id: str, replace_existing: bool = False, max_instances: int = 1):
        """Add or replace one scheduled job."""

    def remove_job(self, job_id: str) -> None:
        """Remove one scheduled job by id."""

    def start(self) -> None:
        """Start the scheduler."""

    def shutdown(self, wait: bool = True) -> None:
        """Stop the scheduler."""


@dataclass(frozen=True)
class ScheduledFlowJob:
    """Description of one scheduler job owned by the host."""

    job_id: str
    flow_name: str
    trigger_kind: str


class SchedulerHost:
    """Own APScheduler timing while delegating flow meaning to the runtime engine."""

    def __init__(
        self,
        *,
        runtime_engine: RuntimeEngine | None = None,
        scheduler: SchedulerPort | None = None,
        job_id_prefix: str = "data-engine:schedule:",
    ) -> None:
        self.runtime_engine = runtime_engine or RuntimeEngine()
        self.scheduler = scheduler or BackgroundScheduler()
        self.job_id_prefix = job_id_prefix
        self._lock = Lock()
        self._job_ids: set[str] = set()

    def rebuild_jobs(self, flows: tuple["Flow", ...]) -> tuple[ScheduledFlowJob, ...]:
        """Replace scheduler jobs from discovered scheduled flows."""
        with self._lock:
            self._remove_known_jobs()
            jobs: list[ScheduledFlowJob] = []
            for flow in flows:
                jobs.extend(self._add_flow_jobs(flow))
            self._job_ids = {job.job_id for job in jobs}
            return tuple(jobs)

    def start(self) -> None:
        """Start the underlying scheduler."""
        self.scheduler.start()

    def shutdown(self, *, wait: bool = True) -> None:
        """Stop the underlying scheduler."""
        self.scheduler.shutdown(wait=wait)

    def _remove_known_jobs(self) -> None:
        for job_id in tuple(self._job_ids):
            try:
                self.scheduler.remove_job(job_id)
            except Exception:
                continue
        self._job_ids.clear()

    def _add_flow_jobs(self, flow: "Flow") -> tuple[ScheduledFlowJob, ...]:
        trigger = flow.trigger
        if not isinstance(trigger, WatchSpec) or trigger.mode != "schedule":
            return ()
        if trigger.interval_seconds is not None:
            job_id = self._job_id(flow, "interval")
            self.scheduler.add_job(
                self._run_flow,
                trigger=IntervalTrigger(seconds=float(trigger.interval_seconds)),
                id=job_id,
                replace_existing=True,
                max_instances=1,
                args=(flow,),
            )
            return (ScheduledFlowJob(job_id=job_id, flow_name=flow.name, trigger_kind="interval"),)
        jobs: list[ScheduledFlowJob] = []
        for hour, minute in trigger.time_slots:
            job_id = self._job_id(flow, f"daily-{hour:02d}-{minute:02d}")
            self.scheduler.add_job(
                self._run_flow,
                trigger=CronTrigger(hour=hour, minute=minute),
                id=job_id,
                replace_existing=True,
                max_instances=1,
                args=(flow,),
            )
            jobs.append(ScheduledFlowJob(job_id=job_id, flow_name=flow.name, trigger_kind="daily"))
        return tuple(jobs)

    def _run_flow(self, flow: "Flow") -> object:
        return self.runtime_engine.run_once(flow)

    def _job_id(self, flow: "Flow", suffix: str) -> str:
        return f"{self.job_id_prefix}{flow.name}:{suffix}"


__all__ = ["ScheduledFlowJob", "SchedulerHost", "SchedulerPort"]
