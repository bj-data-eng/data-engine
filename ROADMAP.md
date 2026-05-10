# Roadmap

## Near Term

- Stabilize the daemon/UI control plane under load.
  Preserve correctness while reducing redundant work at the control seam. Priority work includes adaptive GUI sync backpressure, reducing redundant daemon status chatter, keeping manual runs on a fast-ack path, and continuing to separate immediate control state from eventual persisted history.

- Improve the UI model for parallel and scheduled activity.
  The app should stay honest when several source-scoped runs are active at once. Add better live summary surfaces for concurrent poll/schedule flows, keep stop-state feedback consistent across engine and manual actions, and avoid implying that one flow-level step pane represents one coherent run when activity is parallel.

- Bring the TUI closer to desktop UI parity.
  The desktop UI has become the richer product surface. The TUI should catch up on the most important operator and developer workflows so the two interfaces do not drift.

- Add no-terminal GUI launchers.
  The current `.command` and `.bat` launchers are terminal-oriented. Add platform-appropriate GUI launch paths so the desktop UI can be opened without leaving a console window attached.

- Add PyPDF support.
  PDF-oriented workflows should have a first-class supported dependency and a clearer path for extraction, inspection, and PDF helper utilities inside flow modules.

- Add typed prompting for manual runs.
  Manual flows should eventually be able to request small typed inputs through the UI, such as text, secret, confirm, or path prompts. This should be explicitly limited to manual/operator-driven runs and remain unavailable to unattended poll/schedule engine execution. The first version can keep secrets memory-only for the lifetime of a run, with room later for an optional secrets manager if that becomes necessary.

  The key goal is to preserve the convenience of the current API while fixing the mismatch where directory polling currently implies per-file runtime execution even when the author intends one batch run over the whole folder.

- Continue hardening parallel flow execution.
  Source-scoped concurrency now exists through `watch(..., max_parallel=...)`, but the next work should focus on trustworthiness rather than raw speed: preserve graceful stop semantics, avoid starting queued work after stop is requested, and improve how the operator surface explains concurrent work.

- Add structured per-item batch results.
  Large `map(...)` or `step_each(...)` workflows would benefit from a lightweight success/skip/error result shape so per-item processing can fail, skip, or annotate cleanly without forcing every flow author to invent the same pattern.

- Explore coordinator and worker-process boundaries.
  Heavy manual runs and eventually heavy engine-owned work may benefit from coordinator-plus-worker execution so the control plane remains responsive while expensive flow loading and execution happen elsewhere. Any design here should keep the coordinator authoritative for run reservation, stop state, session visibility, and final runtime truth.

- Define API stabilization boundaries.
  As `watch(...)`, `Flow(label=...)`, `max_parallel`, prompting, and install/runtime changes settle, the project should explicitly separate stable author-facing API from still-evolving surface area.

- Keep profiling lightweight and escalation-based.
  Dev instrumentation should stay sampled and low-noise by default, while heavy tracing such as VizTracer should remain an explicit escalation tool for short repro sessions only.

- Unify scrollbar styling across the UI.
  Scroll behavior and scrollbar appearance should feel more intentional and consistent across the log pane, run-log modal, sidebar, operations pane, and other scrollable surfaces. The current UI has enough scroll surfaces now that scrollbar styling should probably become part of the overall design system instead of staying ad hoc.
