from __future__ import annotations

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from data_engine.authoring.flow import Flow
from data_engine.hosts.scheduler import SchedulerHost


class _FakeScheduler:
    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, object]] = {}
        self.removed: list[str] = []
        self.started = False
        self.shutdown_wait: bool | None = None

    def add_job(self, func, *, trigger, id: str, replace_existing: bool = False, max_instances: int = 1, args=()):
        self.jobs[id] = {
            "func": func,
            "trigger": trigger,
            "replace_existing": replace_existing,
            "max_instances": max_instances,
            "args": args,
        }

    def remove_job(self, job_id: str) -> None:
        self.removed.append(job_id)
        self.jobs.pop(job_id, None)

    def start(self) -> None:
        self.started = True

    def shutdown(self, wait: bool = True) -> None:
        self.shutdown_wait = wait


class _RuntimeEngine:
    def __init__(self) -> None:
        self.flows: list[Flow] = []

    def run_once(self, flow: Flow):
        self.flows.append(flow)
        return [flow.name]


def test_scheduler_host_rebuilds_interval_and_daily_jobs():
    scheduler = _FakeScheduler()
    runtime_engine = _RuntimeEngine()
    host = SchedulerHost(runtime_engine=runtime_engine, scheduler=scheduler)
    interval_flow = Flow(name="every_docs", group="Docs").watch(mode="schedule", interval="10m").step(lambda context: context.current)
    daily_flow = Flow(name="daily_docs", group="Docs").watch(mode="schedule", time=["08:15", "14:45"]).step(lambda context: context.current)
    manual_flow = Flow(name="manual_docs", group="Docs").step(lambda context: context.current)

    jobs = host.rebuild_jobs((interval_flow, daily_flow, manual_flow))

    assert [job.job_id for job in jobs] == [
        "data-engine:schedule:every_docs:interval",
        "data-engine:schedule:daily_docs:daily-08-15",
        "data-engine:schedule:daily_docs:daily-14-45",
    ]
    interval_trigger = scheduler.jobs["data-engine:schedule:every_docs:interval"]["trigger"]
    assert isinstance(interval_trigger, IntervalTrigger)
    assert interval_trigger.interval.total_seconds() == 600
    morning_trigger = scheduler.jobs["data-engine:schedule:daily_docs:daily-08-15"]["trigger"]
    assert isinstance(morning_trigger, CronTrigger)
    assert str(morning_trigger) == "cron[hour='8', minute='15']"
    assert all(job["replace_existing"] is True and job["max_instances"] == 1 for job in scheduler.jobs.values())


def test_scheduler_host_rebuild_removes_previous_jobs():
    scheduler = _FakeScheduler()
    host = SchedulerHost(scheduler=scheduler)
    first = Flow(name="first", group="Docs").watch(mode="schedule", interval="5m").step(lambda context: context.current)
    second = Flow(name="second", group="Docs").watch(mode="schedule", interval="15m").step(lambda context: context.current)

    host.rebuild_jobs((first,))
    host.rebuild_jobs((second,))

    assert scheduler.removed == ["data-engine:schedule:first:interval"]
    assert set(scheduler.jobs) == {"data-engine:schedule:second:interval"}


def test_scheduler_host_job_calls_runtime_engine_run_once():
    scheduler = _FakeScheduler()
    runtime_engine = _RuntimeEngine()
    host = SchedulerHost(runtime_engine=runtime_engine, scheduler=scheduler)
    flow = Flow(name="scheduled", group="Docs").watch(mode="schedule", interval="5m").step(lambda context: context.current)
    host.rebuild_jobs((flow,))
    job = scheduler.jobs["data-engine:schedule:scheduled:interval"]

    result = job["func"](*job["args"])

    assert result == ["scheduled"]
    assert runtime_engine.flows == [flow]


def test_scheduler_host_forwards_lifecycle_to_scheduler():
    scheduler = _FakeScheduler()
    host = SchedulerHost(scheduler=scheduler)

    host.start()
    host.shutdown(wait=False)

    assert scheduler.started is True
    assert scheduler.shutdown_wait is False

