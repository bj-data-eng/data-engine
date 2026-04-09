"""Runtime history query services."""

from __future__ import annotations

from pathlib import Path

from data_engine.domain import FlowCatalogLike, FlowLogEntry, FlowRunState, StepOutputIndex
from data_engine.runtime.runtime_db import RuntimeLedger


class RuntimeHistoryService:
    """Own persisted run/step history queries used by operator surfaces."""

    def rebuild_step_outputs(
        self,
        ledger: RuntimeLedger,
        flow_cards: dict[str, FlowCatalogLike],
    ) -> StepOutputIndex:
        """Rebuild latest successful per-step output paths for visible flows."""
        rebuilt: dict[str, dict[str, Path]] = {}
        for flow_name, card in flow_cards.items():
            outputs: dict[str, Path] = {}
            for run in ledger.list_runs(flow_name=flow_name):
                for step_run in ledger.list_step_runs(run.run_id):
                    if step_run.status != "success" or not step_run.output_path:
                        continue
                    if step_run.step_label not in card.operation_items or step_run.step_label in outputs:
                        continue
                    output_path = Path(str(step_run.output_path))
                    if output_path.exists():
                        outputs[step_run.step_label] = output_path
                if len(outputs) == len(card.operation_items):
                    break
            rebuilt[flow_name] = outputs
        return StepOutputIndex.from_mapping(rebuilt)

    def error_text_for_entry(
        self,
        ledger: RuntimeLedger,
        run_group: FlowRunState,
        entry: FlowLogEntry,
    ) -> tuple[str, str | None]:
        """Return one user-facing error title and persisted error text for a failed entry."""
        run_id = run_group.key[1]
        event = entry.event
        detail_text: str | None = None
        title = "Run Error"
        if event is not None and event.step_name is not None:
            for step_run in ledger.list_step_runs(run_id):
                if step_run.step_label == event.step_name and step_run.status == "failed":
                    detail_text = step_run.error_text
                    title = f"{event.step_name} Error"
                    break
        if detail_text is None:
            for run in ledger.list_runs(flow_name=run_group.key[0]):
                if run.run_id == run_id:
                    detail_text = run.error_text
                    break
        return title, detail_text


__all__ = ["RuntimeHistoryService"]
