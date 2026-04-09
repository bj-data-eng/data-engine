"""Domain models for grouped flow-run state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from data_engine.domain.logs import FlowLogEntry

RunKey = tuple[str, str]


@dataclass(frozen=True)
class RunStepState:
    """One collapsed step state inside a grouped run."""

    step_name: str
    status: str
    elapsed_seconds: float | None
    entry: FlowLogEntry


@dataclass(frozen=True)
class FlowRunState:
    """One grouped run plus its raw log history."""

    key: RunKey
    display_label: str
    source_label: str
    status: str
    elapsed_seconds: float | None
    summary_entry: FlowLogEntry | None
    steps: tuple[RunStepState, ...]
    entries: tuple[FlowLogEntry, ...]

    @classmethod
    def group_entries(cls, entries: tuple[FlowLogEntry, ...]) -> tuple["FlowRunState", ...]:
        """Group flow log entries into one state object per run."""
        if not entries:
            return ()

        run_index_by_id: dict[str, int] = {}
        mutable_runs: list[dict[str, object]] = []

        for entry in entries:
            event = entry.event
            if event is None or event.run_id is None:
                continue

            source_label = event.source_label
            run_token = event.run_id
            if event.step_name is None:
                run_index = run_index_by_id.get(run_token)
                if run_index is None:
                    mutable_runs.append(
                        {
                            "key": (event.flow_name, run_token),
                            "source_label": source_label,
                            "status": event.status,
                            "elapsed_seconds": event.elapsed_seconds,
                            "summary_entry": entry,
                            "steps": [],
                            "entries": [entry],
                        }
                    )
                    run_index_by_id[run_token] = len(mutable_runs) - 1
                else:
                    mutable_run = mutable_runs[run_index]
                    if event.status not in {"success", "finished"} or mutable_run["status"] not in {"failed", "stopped"}:
                        mutable_run["status"] = event.status
                        mutable_run["summary_entry"] = entry
                    mutable_run["elapsed_seconds"] = event.elapsed_seconds
                    run_entries = mutable_run["entries"]
                    assert isinstance(run_entries, list)
                    run_entries.append(entry)
                continue

            run_index = run_index_by_id.get(run_token)
            if run_index is None:
                mutable_runs.append(
                    {
                        "key": (event.flow_name, run_token),
                        "source_label": source_label,
                        "status": "started",
                        "elapsed_seconds": None,
                        "summary_entry": None,
                        "steps": [],
                        "entries": [],
                    }
                )
                run_index = len(mutable_runs) - 1
                run_index_by_id[run_token] = run_index

            mutable_run = mutable_runs[run_index]
            steps = mutable_run["steps"]
            assert isinstance(steps, list)
            run_entries = mutable_run["entries"]
            assert isinstance(run_entries, list)
            run_entries.append(entry)
            step_key = (event.flow_name, event.step_name, source_label)
            step_state = RunStepState(
                step_name=event.step_name,
                status=event.status,
                elapsed_seconds=event.elapsed_seconds,
                entry=entry,
            )
            if event.status in {"success", "failed", "stopped"}:
                for index in range(len(steps) - 1, -1, -1):
                    candidate = steps[index]
                    if (
                        isinstance(candidate, RunStepState)
                        and candidate.entry.event is not None
                        and candidate.entry.event.step_name is not None
                        and (
                            candidate.entry.event.flow_name,
                            candidate.entry.event.step_name,
                            candidate.entry.event.source_label,
                        )
                        == step_key
                        and candidate.entry.event.status == "started"
                    ):
                        steps[index] = step_state
                        break
                else:
                    steps.append(step_state)
            else:
                steps.append(step_state)

        return tuple(
            cls(
                key=run["key"],
                display_label=cls._display_label_for_run(run),
                source_label=run["source_label"],
                status=run["status"],
                elapsed_seconds=run["elapsed_seconds"],
                summary_entry=run["summary_entry"],
                steps=tuple(run["steps"]),
                entries=tuple(run["entries"]),
            )
            for run in mutable_runs
        )

    @staticmethod
    def _display_label_for_run(run: dict[str, object]) -> str:
        summary_entry = run.get("summary_entry")
        if isinstance(summary_entry, FlowLogEntry):
            created_at = summary_entry.created_at_utc
        else:
            run_entries = run.get("entries")
            first_entry = run_entries[0] if isinstance(run_entries, list) and run_entries else None
            created_at = first_entry.created_at_utc if isinstance(first_entry, FlowLogEntry) else datetime.now(UTC)
        return created_at.astimezone().strftime("%Y-%m-%d %I:%M:%S %p")


__all__ = ["FlowRunState", "RunKey", "RunStepState"]
