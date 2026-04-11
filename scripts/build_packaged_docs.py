#!/usr/bin/env python3
"""Build packaged HTML docs into the installed package tree."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIR = PROJECT_ROOT / "src" / "data_engine" / "docs" / "sphinx_source"
OUTPUT_DIR = PROJECT_ROOT / "src" / "data_engine" / "docs" / "html"
PROJECT_AST_MAP_SCRIPT = PROJECT_ROOT / "src" / "data_engine" / "devtools" / "project_ast_map.py"


def build_packaged_docs() -> Path:
    """Build packaged HTML docs and return the output directory."""
    subprocess.run(
        [
            sys.executable,
            str(PROJECT_AST_MAP_SCRIPT),
            str(PROJECT_ROOT / "src" / "data_engine"),
            "--write-docs",
            str(SOURCE_DIR / "guides"),
        ],
        check=True,
    )
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "sphinx",
            "-b",
            "html",
            str(SOURCE_DIR),
            str(OUTPUT_DIR),
        ],
        check=True,
    )
    return OUTPUT_DIR


def main() -> int:
    output_dir = build_packaged_docs()
    print(output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
