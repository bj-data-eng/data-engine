# Roadmap

## Near Term

- Bring the TUI closer to desktop UI parity.
  The desktop UI has become the richer product surface. The TUI should catch up on the most important operator and developer workflows so the two interfaces do not drift.

- Add no-terminal GUI launchers.
  The current `.command` and `.bat` launchers are terminal-oriented. Add platform-appropriate GUI launch paths so the desktop UI can be opened without leaving a console window attached.


- Add PyPDF support.
  PDF-oriented workflows should have a first-class supported dependency and a clearer path for extraction, inspection, and PDF helper utilities inside flow modules.

- Add typed prompting for manual runs.
  Manual flows should eventually be able to request small typed inputs through the UI, such as text, secret, confirm, or path prompts. This should be explicitly limited to manual/operator-driven runs and remain unavailable to unattended poll/schedule engine execution. The first version can keep secrets memory-only for the lifetime of a run, with room later for an optional secrets manager if that becomes necessary.

  The key goal is to preserve the convenience of the current API while fixing the mismatch where directory polling currently implies per-file runtime execution even when the author intends one batch run over the whole folder.

- Add atomic file-write helpers.
  Downstream polling and inspection will be more reliable if common output paths can use a temp-write-then-rename pattern instead of exposing partially written files at their final paths.


- Add structured per-item batch results.
  Large `map(...)` or `step_each(...)` workflows would benefit from a lightweight success/skip/error result shape so per-item processing can fail, skip, or annotate cleanly without forcing every flow author to invent the same pattern.

- Define the real install/runtime story for non-editable installs.
  PyPI-style installation will need a clear first-run shape that covers dependency installation assumptions, workspace-root setup, bootstrap, cache/runtime path creation, and launcher behavior.

- Define API stabilization boundaries.
  As `watch(...)`, `Flow(label=...)`, prompting, and install/runtime changes settle, the project should explicitly separate stable author-facing API from still-evolving surface area.

- Add a DuckDB recipe layer.
  The project should document and possibly lightly support common DuckDB workflow patterns such as raw-ingest tables, replace-by-key updates, derived-table refreshes, and Polars-plus-DuckDB transforms so users can build database-backed flows more consistently.

- Unify scrollbar styling across the UI.
  Scroll behavior and scrollbar appearance should feel more intentional and consistent across the log pane, run-log modal, sidebar, operations pane, and other scrollable surfaces. The current UI has enough scroll surfaces now that scrollbar styling should probably become part of the overall design system instead of staying ad hoc.

- Consider inlining UI icons.
  The app currently ships SVG icon assets as package data. Revisit whether some or all icons should be inlined in code/resources to simplify packaging, installation, and eventual distribution changes.
