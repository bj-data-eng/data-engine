#!/usr/bin/env python3
"""Register the active Python environment as the shared Data Engine notebook kernel."""

from __future__ import annotations

import subprocess
import sys


def main() -> None:
    """Install or update the shared Data Engine ipykernel entry."""
    subprocess.run(
        [
            sys.executable,
            "-m",
            "ipykernel",
            "install",
            "--user",
            "--name",
            "data-engine",
            "--display-name",
            "Data Engine (.venv)",
        ],
        check=True,
    )
    print("Registered notebook kernel: Data Engine (.venv)")


if __name__ == "__main__":
    main()
