"""Runtime history query services."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from data_engine.domain import FlowCatalogLike, FlowLogEntry, FlowRunState, StepOutputIndex
from data_engine.runtime.ledger_models import PersistedStepRun
from data_engine.services.runtime_ports import RuntimeCacheStore


@dataclass(frozen=True)
class StepOutputRefresh:
    """One step-output refresh result with cache watermark."""

    last_step_run_id: int | None
    index: StepOutputIndex


class RuntimeHistoryService:
    """Own persisted run/step history queries used by operator surfaces."""

    def rebuild_step_outputs(
        self,
        ledger: RuntimeCacheStore,
        flow_cards: dict[str, FlowCatalogLike],
    ) -> StepOutputRefresh:
        """Rebuild latest successful per-step output paths for visible flows."""
        rebuilt: dict[str, dict[str, Path]] = {}
        last_step_run_id: int | None = None
        for flow_name, card in flow_cards.items():
            outputs: dict[str, Path] = {}
            step_runs = ledger.step_outputs.list(flow_name=flow_name)
            if step_runs:
                candidate_last_id = step_runs[-1].id
                last_step_run_id = candidate_last_id if last_step_run_id is None else max(last_step_run_id, candidate_last_id)
            for step_run in reversed(step_runs):
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
        return StepOutputRefresh(
            last_step_run_id=last_step_run_id,
            index=StepOutputIndex.from_mapping(rebuilt),
        )

    def refresh_step_outputs(
        self,
        ledger: RuntimeCacheStore,
        flow_cards: dict[str, FlowCatalogLike],
        *,
        current_index: StepOutputIndex,
        last_seen_step_run_id: int | None,
    ) -> StepOutputRefresh:
        """Incrementally merge newly finished successful step outputs into the current index."""
        new_rows = ledger.step_outputs.list(after_id=last_seen_step_run_id)
        if not new_rows:
            return StepOutputRefresh(last_step_run_id=last_seen_step_run_id, index=current_index)
        outputs_by_flow = {
            flow_name: dict(current_index.outputs_for(flow_name).outputs)
            for flow_name in flow_cards
        }
        next_last_id = last_seen_step_run_id
        for step_run in new_rows:
            next_last_id = step_run.id if next_last_id is None else max(next_last_id, step_run.id)
            self._merge_step_output_row(outputs_by_flow, flow_cards, step_run)
        return StepOutputRefresh(
            last_step_run_id=next_last_id,
            index=StepOutputIndex.from_mapping(outputs_by_flow),
        )

    def _merge_step_output_row(
        self,
        outputs_by_flow: dict[str, dict[str, Path]],
        flow_cards: dict[str, FlowCatalogLike],
        step_run: PersistedStepRun,
    ) -> None:
        card = flow_cards.get(step_run.flow_name)
        if card is None or step_run.status != "success" or not step_run.output_path:
            return
        if step_run.step_label not in card.operation_items:
            return
        output_path = Path(str(step_run.output_path))
        if not output_path.exists():
            return
        outputs_by_flow.setdefault(step_run.flow_name, {})[step_run.step_label] = output_path

    def error_text_for_entry(
        self,
        ledger: RuntimeCacheStore,
        run_group: FlowRunState,
        entry: FlowLogEntry,
    ) -> tuple[str, str | None]:
        """Return one user-facing error title and persisted error text for a failed entry."""
        refresh_external_state = getattr(ledger, "refresh_external_state", None)
        if callable(refresh_external_state):
            refresh_external_state()
        run_id = run_group.key[1]
        event = entry.event
        detail_text: str | None = None
        title = "Run Error"
        if event is not None and event.step_name is not None:
            for step_run in ledger.step_outputs.list_for_run(run_id):
                if step_run.step_label == event.step_name and step_run.status == "failed":
                    detail_text = step_run.error_text
                    title = f"{event.step_name} Error"
                    break
        if detail_text is None:
            for run in ledger.runs.list(flow_name=run_group.key[0]):
                if run.run_id == run_id:
                    detail_text = run.error_text
                    break
        return title, detail_text


__all__ = ["RuntimeHistoryService", "StepOutputRefresh"]
