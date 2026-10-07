from threading import Event, Thread

from data_engine.authoring.flow import Flow
from data_engine.hosts.scheduler import SchedulerHost
from data_engine.services.runtime_execution import RuntimeExecutionService


def test_graceful_stop_rejects_real_scheduler_ticks_while_polling_drains():
    runtime_stop, flow_stop = Event(), Event()
    polling_started, release_polling = Event(), Event()
    scheduled_started, release_scheduled = Event(), Event()
    rejected_tick = Event()
    calls = []
    results = []
    hosts = []

    class Engine:
        def __init__(self, **kwargs):
            self.flow_stop = kwargs.get("flow_stop_event")

        def run_grouped(self, flows, continuous):
            polling_started.set()
            assert release_polling.wait(5)
            return ["poll finished"]

        def run_once(self, flow):
            calls.append(flow.name)
            assert not self.flow_stop.is_set()
            scheduled_started.set()
            assert release_scheduled.wait(5)
            return ["schedule finished"]

    class ObservedHost(SchedulerHost):
        def _run_flow(self, flow):
            result = super()._run_flow(flow)
            if runtime_stop.is_set() and result is None:
                rejected_tick.set()
            return result

    def factory(**kwargs):
        host = ObservedHost(**kwargs)
        hosts.append(host)
        return host

    service = RuntimeExecutionService(runtime_engine_type=Engine, scheduler_host_factory=factory)
    poll = Flow(name="poll", group="Docs").watch(mode="poll", source="input", interval="5s").step(lambda context: 1)
    scheduled = Flow(name="schedule", group="Docs").watch(mode="schedule", interval="0.05s").step(lambda context: 1)
    worker = Thread(target=lambda: results.append(service.run_automated(
        (poll, scheduled), runtime_stop_event=runtime_stop, flow_stop_event=flow_stop,
    )))
    worker.start()
    try:
        assert polling_started.wait(3)
        assert scheduled_started.wait(3)
        runtime_stop.set()
        assert worker.is_alive()
        assert not flow_stop.is_set()
        release_scheduled.set()
        assert rejected_tick.wait(3)
        assert calls == ["schedule"]
        assert worker.is_alive()
        assert hosts[0].scheduler.running
        release_polling.set()
    finally:
        release_scheduled.set()
        release_polling.set()
        worker.join(5)
    assert not worker.is_alive()
    assert not hosts[0].scheduler.running
    assert results == [["poll finished"]]
