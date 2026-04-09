"""Print resolved Data Engine workspace paths for shell/install scripts."""

from __future__ import annotations

import sys

from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


_RUNTIME_LAYOUT_POLICY = RuntimeLayoutPolicy()


def main(argv: list[str] | None = None) -> int:
    """Print one resolved workspace path selected by key."""
    args = list(sys.argv[1:] if argv is None else argv)
    key = args[0] if args else "data_root"
    workspace = _RUNTIME_LAYOUT_POLICY.resolve_paths()

    mapping = {
        "data_root": workspace.workspace_root,
        "workspace_root": workspace.workspace_root,
        "config_dir": workspace.config_dir,
        "flow_modules_dir": workspace.flow_modules_dir,
        "compiled_flow_modules_dir": workspace.compiled_flow_modules_dir,
        "databases_dir": workspace.databases_dir,
        "artifacts_dir": workspace.artifacts_dir,
        "runtime_state_dir": workspace.runtime_state_dir,
        "runtime_db_path": workspace.runtime_db_path,
        "documentation_dir": workspace.documentation_dir,
        "documentation_source_dir": workspace.sphinx_source_dir,
        "sphinx_source_dir": workspace.sphinx_source_dir,
        "documentation_build_dir": workspace.documentation_dir / "_build" / "html",
    }

    try:
        target = mapping[key]
    except KeyError:
        valid = ", ".join(sorted(mapping))
        raise SystemExit(f"Unknown workspace path key: {key!r}. Valid keys: {valid}")

    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
