"""Continuous polling loop for one sequential flow runtime."""

from __future__ import annotations

from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from time import monotonic, sleep
from typing import TYPE_CHECKING

from data_engine.runtime.execution.context import QueuedRunJob
from data_engine.core.primitives import WatchSpec
from data_engine.runtime.file_watch import PollingWatcher

if TYPE_CHECKING:
    from data_engine.runtime.execution.single import FlowRuntime
    from data_engine.core.primitives import FlowContext


class ContinuousRuntimeLoop:
    """Own the polling loop for one sequential runtime."""

    def __init__(self, runtime: "FlowRuntime") -> None:
        self.runtime = runtime

    def run(self) -> list["FlowContext"]:
        results: list["FlowContext"] = []
        queue: deque[QueuedRunJob] = deque()
        queued_keys: set[tuple[str, str | None]] = set()
        pending_futures: dict[Future[FlowContext], tuple[QueuedRunJob, int]] = {}
        watch_entries: list[dict[str, object]] = []
        now = monotonic()
        max_workers = max(sum(self.runtime.max_parallel_for_flow(flow) for flow in self.runtime.flows), 1)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for flow in self.runtime.flows:
                if flow.trigger is None:
                    for source_path in self.runtime.polling.startup_sources(flow):
                        self.runtime.polling.enqueue_job(queue, queued_keys, flow, source_path)
                    continue
                if isinstance(flow.trigger, WatchSpec) and flow.trigger.mode == "poll":
                    watcher = self.runtime.polling.make_watcher(flow.trigger)
                    watcher.start()
                    for source_path in self.runtime.polling.stale_poll_sources(flow):
                        batch_signatures = self.runtime.polling.stale_batch_poll_signatures(flow) if source_path is None else ()
                        self.runtime.polling.enqueue_job(queue, queued_keys, flow, source_path, batch_signatures=batch_signatures)
                    watch_entries.append(
                        {
                            "flow": flow,
                            "interval": flow.trigger.interval_seconds,
                            "next_poll": now + float(flow.trigger.interval_seconds),
                            "watcher": watcher,
                        }
                    )

            self.runtime._emit_status("Polling watcher running.")
            try:
                while True:
                    if self.runtime.runtime_stop_event is not None and self.runtime.runtime_stop_event.is_set():
                        self.runtime._emit_status("Polling watcher stopped.")
                        break
                    now = monotonic()
                    self._poll_watch_entries(watch_entries, queue, queued_keys, now)
                    self.runtime.dispatch_queued_jobs(
                        queue,
                        queued_keys,
                        pending_futures,
                        executor,
                        results=None,
                    )
                    if queue or pending_futures:
                        self._wait_for_activity(
                            watch_entries=watch_entries,
                            pending_futures=pending_futures,
                        )
                        continue
                    self._sleep_until_next_poll(watch_entries)
            finally:
                self.runtime.wait_for_dispatched_jobs(pending_futures, results=None)
                for entry in watch_entries:
                    watcher = entry["watcher"]
                    if isinstance(watcher, PollingWatcher):
                        watcher.stop()
        return results

    def _wait_for_activity(
        self,
        *,
        watch_entries: list[dict[str, object]],
        pending_futures: dict[Future["FlowContext"], tuple[QueuedRunJob, int]],
    ) -> None:
        """Block until either a queued run finishes or the next watcher poll is due."""
        timeout_seconds = self._next_poll_timeout_seconds(watch_entries)
        if not pending_futures:
            if timeout_seconds is None:
                sleep(0.05)
            elif timeout_seconds > 0.0:
                sleep(timeout_seconds)
            return
        wait(
            tuple(pending_futures),
            timeout=timeout_seconds,
            return_when=FIRST_COMPLETED,
        )

    def _sleep_until_next_poll(self, watch_entries: list[dict[str, object]]) -> None:
        """Sleep for a bounded interval while the continuous runtime is idle."""
        timeout_seconds = self._next_poll_timeout_seconds(watch_entries)
        if timeout_seconds is None:
            sleep(0.05)
            return
        if timeout_seconds > 0.0:
            sleep(timeout_seconds)

    def _next_poll_timeout_seconds(self, watch_entries: list[dict[str, object]]) -> float | None:
        """Return how long the loop can wait before the next watcher poll is due."""
        if not watch_entries:
            return None
        now = monotonic()
        next_poll_at = min(float(entry["next_poll"]) for entry in watch_entries)
        return max(next_poll_at - now, 0.0)

    def _poll_watch_entries(
        self,
        watch_entries: list[dict[str, object]],
        queue: deque[QueuedRunJob],
        queued_keys: set[tuple[str, str | None]],
        now: float,
    ) -> None:
        for entry in watch_entries:
            if now < entry["next_poll"]:
                continue
            watched_flow = entry["flow"]
            watcher = entry["watcher"]
            assert isinstance(watcher, PollingWatcher)
            for path in watcher.drain_events():
                watched_trigger = watched_flow.trigger
                assert isinstance(watched_trigger, WatchSpec)
                if watched_trigger.run_as == "batch" and watched_trigger.source is not None and watched_trigger.source.is_dir():
                    signature = self.runtime.polling.poll_source_signature(watched_flow, path)
                    self.runtime.polling.enqueue_job(
                        queue,
                        queued_keys,
                        watched_flow,
                        None,
                        batch_signatures=(signature,) if signature is not None else (),
                    )
                    break
                if self.runtime.polling.is_poll_source_stale(watched_flow, path):
                    self.runtime.polling.enqueue_job(queue, queued_keys, watched_flow, path)
            entry["next_poll"] = now + float(entry["interval"])

__all__ = ["ContinuousRuntimeLoop"]
