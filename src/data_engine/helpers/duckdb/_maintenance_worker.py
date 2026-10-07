"""Isolated schema maintenance holding DuckDB's exclusive database file lock."""

from __future__ import annotations

import json
from pathlib import Path
import sys

from data_engine.helpers.duckdb._maintenance import _compact_database_in_process


def main(request_json: str, output: Path) -> int:
    """Run one maintenance request and publish its result or structured failure."""
    request = json.loads(request_json)
    try:
        frame = _compact_database_in_process(**request)
        frame.write_ipc(output)
    except BaseException as exc:
        output.with_suffix(".error.json").write_text(
            json.dumps({"type": type(exc).__name__, "message": str(exc)}), encoding="utf-8",
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1], Path(sys.argv[2])))
