from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist


ROOT = Path(__file__).resolve().parent
DOCS_SOURCE_DIR = ROOT / "src" / "data_engine" / "docs" / "sphinx_source"
DOCS_OUTPUT_DIR = ROOT / "src" / "data_engine" / "docs" / "html"
PROJECT_AST_MAP_SCRIPT = ROOT / "src" / "data_engine" / "devtools" / "project_ast_map.py"


def _build_packaged_docs() -> None:
    if not DOCS_SOURCE_DIR.is_dir():
        raise FileNotFoundError(f"Packaged docs source directory is missing: {DOCS_SOURCE_DIR}")
    subprocess.run(
        [
            sys.executable,
            str(PROJECT_AST_MAP_SCRIPT),
            str(ROOT / "src" / "data_engine"),
            "--write-docs",
            str(DOCS_SOURCE_DIR / "guides"),
        ],
        check=True,
    )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "sphinx",
            "-b",
            "html",
            str(DOCS_SOURCE_DIR),
            str(DOCS_OUTPUT_DIR),
        ],
        check=True,
    )


class build_py(_build_py):
    def run(self) -> None:
        _build_packaged_docs()
        super().run()


class sdist(_sdist):
    def run(self) -> None:
        _build_packaged_docs()
        super().run()


setup(cmdclass={"build_py": build_py, "sdist": sdist})
