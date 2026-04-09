from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist


ROOT = Path(__file__).resolve().parent


def _build_packaged_docs() -> None:
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "build_packaged_docs.py")],
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
