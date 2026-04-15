"""Flow-module compilation and mirroring for Data Engine modules."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import tempfile

from data_engine.core.model import FlowValidationError
from data_engine.platform.workspace_models import WORKSPACE_FLOW_HELPERS_DIR_NAME
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


@dataclass(frozen=True)
class CompiledFlowModule:
    """Information about one compiled flow module."""

    name: str
    source_path: Path
    module_path: Path


def compile_stale_flow_module_notebooks(
    *,
    data_root: Path | None = None,
) -> tuple[CompiledFlowModule, ...]:
    """Compile notebook flow modules and mirror authored Python modules into compiled output."""
    flow_modules_dir, modules_dir = resolve_flow_module_paths(
        data_root=data_root,
    )
    modules_dir.mkdir(parents=True, exist_ok=True)

    if not flow_modules_dir.exists():
        return ()

    notebook_paths = sorted(flow_modules_dir.glob("*.ipynb"))
    python_paths = sorted(path for path in flow_modules_dir.glob("*.py") if path.name != "__init__.py")
    _validate_unique_authored_flow_module_stems(notebook_paths, python_paths)

    helper_modules_dir = flow_modules_dir / WORKSPACE_FLOW_HELPERS_DIR_NAME
    compiled_helper_modules_dir = modules_dir / WORKSPACE_FLOW_HELPERS_DIR_NAME
    authored_names = {path.stem for path in notebook_paths} | {path.stem for path in python_paths}
    _remove_orphaned_compiled_modules(modules_dir, authored_names)
    _mirror_helper_modules(helper_modules_dir, compiled_helper_modules_dir)

    compiled: list[CompiledFlowModule] = []
    for notebook_path in notebook_paths:
        module_path = modules_dir / f"{notebook_path.stem}.py"
        if compile_flow_module_notebook(notebook_path, module_path):
            compiled.append(CompiledFlowModule(name=notebook_path.stem, source_path=notebook_path, module_path=module_path))
    for source_path in python_paths:
        module_path = modules_dir / source_path.name
        if mirror_flow_module_python_module(source_path, module_path):
            compiled.append(CompiledFlowModule(name=source_path.stem, source_path=source_path, module_path=module_path))
    return tuple(compiled)


def resolve_flow_module_paths(
    *,
    data_root: Path | None = None,
) -> tuple[Path, Path]:
    """Resolve the authored flow-module and compiled output directories."""
    workspace = RuntimeLayoutPolicy().resolve_paths(data_root=data_root)
    return workspace.flow_modules_dir, workspace.compiled_flow_modules_dir


def compile_flow_module_notebook(notebook_path: Path, module_path: Path) -> bool:
    """Compile one notebook-authored flow module into a Python module."""
    rendered = _render_compiled_notebook_module(notebook_path)
    return _write_module_text_if_changed(module_path, rendered)


def _render_compiled_notebook_module(notebook_path: Path) -> str:
    """Render one notebook-authored flow module into runtime Python text."""
    payload = json.loads(notebook_path.read_text(encoding="utf-8"))
    cells = payload.get("cells")
    if not isinstance(cells, list):
        raise FlowValidationError(f"Notebook cells payload is invalid in {notebook_path}")

    code_blocks: list[str] = []
    for cell in cells:
        if not isinstance(cell, dict) or cell.get("cell_type") != "code":
            continue
        source = cell.get("source", [])
        if isinstance(source, str):
            text = source
        elif isinstance(source, list) and all(isinstance(line, str) for line in source):
            text = "".join(source)
        else:
            raise FlowValidationError(f"Notebook code cell source is invalid in {notebook_path}")
        stripped = text.strip()
        if not stripped:
            continue
        for line in stripped.splitlines():
            if line.lstrip().startswith("%") or line.lstrip().startswith("!"):
                raise FlowValidationError(f"Notebook magics and shell commands are not allowed in {notebook_path}")
        code_blocks.append(stripped)

    if not code_blocks:
        raise FlowValidationError(f"Notebook does not contain any code cells to compile: {notebook_path}")

    rendered = [
        '"""Auto-compiled flow module. Source notebook is authoritative."""',
        "",
        "from __future__ import annotations",
        "",
        f"# Source notebook: {notebook_path.as_posix()}",
        "",
    ]
    rendered.append("\n\n".join(code_blocks))
    rendered.append("")
    return "\n".join(rendered)


def mirror_flow_module_python_module(source_path: Path, module_path: Path) -> bool:
    """Mirror one authored Python flow/helper module into compiled output."""
    rendered = _render_mirrored_python_module(source_path)
    return _write_module_text_if_changed(module_path, rendered)


def _render_mirrored_python_module(source_path: Path) -> str:
    """Render the mirrored runtime text for one authored Python flow module."""
    source_text = source_path.read_text(encoding="utf-8")
    rendered = [
        f"# Mirrored flow module. Source file is authoritative: {source_path.as_posix()}",
        "",
        source_text.rstrip(),
        "",
    ]
    return "\n".join(rendered)


def _write_module_text_if_changed(module_path: Path, rendered: str) -> bool:
    """Write one compiled module only when its rendered content changed."""
    try:
        existing = module_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        existing = None
    if existing == rendered:
        return False
    module_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(module_path, rendered)
    return True


def _atomic_write_text(path: Path, text: str) -> None:
    """Write text to one path via a same-directory temporary file and replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(text)
        temp_path = Path(handle.name)
    temp_path.replace(path)


def _atomic_copy_file(source_path: Path, target_path: Path) -> None:
    """Copy one file into place without replacing the containing directory."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("wb", dir=target_path.parent, delete=False) as handle:
        temp_path = Path(handle.name)
    try:
        shutil.copy2(source_path, temp_path)
        temp_path.replace(target_path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def _mirror_helper_modules(helper_modules_dir: Path, compiled_helper_modules_dir: Path) -> None:
    """Mirror authored helper modules into compiled output as an importable package."""
    if not helper_modules_dir.is_dir():
        if compiled_helper_modules_dir.exists():
            shutil.rmtree(compiled_helper_modules_dir)
        return
    compiled_helper_modules_dir.mkdir(parents=True, exist_ok=True)
    authored_relative_paths = {
        path.relative_to(helper_modules_dir)
        for path in helper_modules_dir.rglob("*")
    }
    for relative_path in sorted(authored_relative_paths):
        source_path = helper_modules_dir / relative_path
        target_path = compiled_helper_modules_dir / relative_path
        if source_path.is_dir():
            target_path.mkdir(parents=True, exist_ok=True)
            continue
        _atomic_copy_file(source_path, target_path)

    init_path = compiled_helper_modules_dir / "__init__.py"
    if not init_path.exists():
        _atomic_write_text(init_path, '"""Authored flow helper modules for flow-module imports."""\n')

    expected_paths = set(authored_relative_paths)
    expected_paths.add(Path("__init__.py"))
    for existing_path in sorted(compiled_helper_modules_dir.rglob("*"), key=lambda path: (len(path.parts), str(path)), reverse=True):
        relative_path = existing_path.relative_to(compiled_helper_modules_dir)
        if relative_path in expected_paths:
            continue
        if existing_path.is_dir():
            existing_path.rmdir()
        else:
            existing_path.unlink()


def _validate_unique_authored_flow_module_stems(notebook_paths: list[Path], python_paths: list[Path]) -> None:
    """Reject authored flow-module directories that define the same module stem twice."""
    notebook_stems = {path.stem for path in notebook_paths}
    python_stems = {path.stem for path in python_paths}
    overlaps = sorted(notebook_stems & python_stems)
    if overlaps:
        names = ", ".join(overlaps)
        raise FlowValidationError(f"Flow module sources conflict between .ipynb and .py files: {names}")


def _remove_orphaned_compiled_modules(modules_dir: Path, authored_names: set[str]) -> None:
    """Delete generated modules and caches that no longer have a notebook source."""
    for module_path in modules_dir.glob("*.py"):
        if module_path.name == "__init__.py" or module_path.stem.startswith("_"):
            continue
        if module_path.stem not in authored_names and _is_generated_module(module_path):
            module_path.unlink()

    pycache_dir = modules_dir / "__pycache__"
    if pycache_dir.exists():
        shutil.rmtree(pycache_dir)


def _is_generated_module(module_path: Path) -> bool:
    """Return whether a compiled module was generated or mirrored from authored sources."""
    try:
        first_line = module_path.read_text(encoding="utf-8").splitlines()[0]
    except (FileNotFoundError, IndexError):
        return False
    return "Auto-compiled flow module" in first_line or "Mirrored flow module" in first_line


__all__ = [
    "CompiledFlowModule",
    "compile_flow_module_notebook",
    "compile_stale_flow_module_notebooks",
    "mirror_flow_module_python_module",
    "resolve_flow_module_paths",
]
