# Agent Guide

This directory contains the Rust-backed `egui` surface for Data Engine. Use
these instructions when working inside `rust/egui_surface/`.

## Scope

- Keep changes in this crate focused on the Rust `egui` surface and its Python
  extension boundary.
- Prefer default `egui` conventions and native Rust patterns over porting Qt
  widget/styling concepts directly.
- Treat this crate as the renderer layer. Keep runtime/control ownership in the
  Python/Data Engine service architecture unless the boundary is being
  intentionally changed.

## Build And Run

- Build the Python extension into the repo venv:

```powershell
.\.venv\Scripts\python.exe -m maturin develop --manifest-path rust\egui_surface\Cargo.toml
```

- Launch the `egui` surface directly:

```powershell
.\.venv\Scripts\python.exe -m data_engine.ui.egui.launcher
```

- Fast Rust-only iteration from this directory:

```powershell
cargo check
cargo fmt
cargo clippy --all-targets --all-features
cargo nextest run
```

## Fast Loop Tools

- Use `cargo watch -x check` for continuous compile feedback.
- Use `bacon` for a richer live dashboard.
- Use `cargo expand` when debugging `pyo3` macros or generated code.
- Use `cargo tree` to inspect dependency shape.
- Use `cargo outdated --locked` for dependency review.

## UI Guidance

- Default to `egui` layout primitives:
  - `TopBottomPanel`
  - `SidePanel`
  - `CentralPanel`
  - `ScrollArea`
  - `Grid`
  - `CollapsingHeader`
- Give explicit IDs to repeated `egui` widgets:
  - repeated `ScrollArea`
  - repeated `CollapsingHeader`
  - repeated rows/items using `ui.push_id(...)`
- Avoid introducing custom styling too early. First make the shell and data
  flow work with stock `egui` behavior.
- Favor deterministic sample or projected state models over ad hoc mutable UI
  state.

## Python Extension Boundary

- The crate builds a `.pyd` via `pyo3`/`maturin`, similar to the Polars native
  packaging model.
- Keep the Python-facing API narrow and explicit:
  - `hello()`
  - `runtime_info()`
  - `launch(...)`
- Do not pass complex live Python objects into Rust unless the boundary is being
  intentionally redesigned. Prefer simple values and explicit data translation.
- Preserve compatibility with the Python wrapper in:
  - `src/data_engine/ui/egui/native.py`
  - `src/data_engine/ui/egui/launcher.py`

## Validation

- Before wrapping up Rust-side changes, run:

```powershell
.\.venv\Scripts\python.exe -m maturin develop --manifest-path rust\egui_surface\Cargo.toml
.\.venv\Scripts\ruff.exe check src\data_engine\ui\egui tests\egui
.\.venv\Scripts\python.exe -m pytest -q tests\egui tests\cli\test_start.py tests\cli\test_doctor.py
```

- If the change is Rust-only and does not affect Python wrapper behavior, still
  rebuild the extension before reporting completion.

## Git Hygiene

- Do not commit Rust build outputs such as `target/`.
- Keep edits focused; do not revert unrelated repo work.
- Prefer incremental renderer migration over reintroducing Qt dependencies into
  this crate.
