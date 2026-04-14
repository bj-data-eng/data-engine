from __future__ import annotations

from data_engine.runtime.execution.logging import RuntimeLogEmitter, acquire_queued_runtime_log_sink


class _FakeLogSink:
    def __init__(self) -> None:
        self.rows = []
        self.append_calls = 0
        self.append_many_calls: list[int] = []

    def append(
        self,
        *,
        level: str,
        message: str,
        created_at_utc: str,
        run_id: str | None = None,
        flow_name: str | None = None,
        step_label: str | None = None,
    ) -> None:
        self.append_calls += 1
        self.rows.append((level, message, created_at_utc, run_id, flow_name, step_label))

    def append_many(self, rows) -> None:
        self.append_many_calls.append(len(rows))
        self.rows.extend(
            (row.level, row.message, row.created_at_utc, row.run_id, row.flow_name, row.step_label)
            for row in rows
        )


def test_queued_runtime_log_sink_flushes_shared_batches_on_last_close():
    sink = _FakeLogSink()
    first = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001, max_batch_size=100)
    second = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001, max_batch_size=100)
    first_emitter = RuntimeLogEmitter(first)
    second_emitter = RuntimeLogEmitter(second)

    first_emitter.log_runtime_message("first", level="info", run_id="run-1", flow_name="claims_poll", step_label="Read Excel")
    second_emitter.log_runtime_message("second", level="info", run_id="run-1", flow_name="claims_poll", step_label="Write Parquet")

    first.close()
    assert sink.rows == []

    second.close()

    assert [row[1] for row in sink.rows] == ["first", "second"]
    assert sink.append_calls == 0
    assert sum(sink.append_many_calls) == 2


def test_queued_runtime_log_sink_can_be_reacquired_after_last_close():
    sink = _FakeLogSink()
    first = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001, max_batch_size=100)
    RuntimeLogEmitter(first).log_runtime_message("first", level="info", run_id="run-1", flow_name="claims_poll")
    first.close()

    second = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001, max_batch_size=100)
    RuntimeLogEmitter(second).log_runtime_message("second", level="info", run_id="run-2", flow_name="claims_poll")
    second.close()

    assert [row[1] for row in sink.rows] == ["first", "second"]
