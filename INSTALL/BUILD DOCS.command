#!/bin/zsh

set -euo pipefail

SCRIPT_PATH="${0:A}"
PROJECT_ROOT="$(cd "$(dirname "$SCRIPT_PATH")/.." && pwd -P)"
VENV_DIR="$PROJECT_ROOT/.venv"
VENV_PYTHON="$VENV_DIR/bin/python"

if [[ ! -f "$PROJECT_ROOT/pyproject.toml" ]]; then
  echo "Could not locate pyproject.toml next to the docs builder."
  echo "Expected project root: $PROJECT_ROOT"
  echo
  if [[ -t 0 ]]; then
    read "?Press return to close..."
  fi
  exit 1
fi

cd "$PROJECT_ROOT"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python)"
else
  echo "Python was not found on PATH."
  echo
  if [[ -t 0 ]]; then
    read "?Press return to close..."
  fi
  exit 1
fi

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "Creating virtual environment..."
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

echo "Installing docs build dependencies..."
"$VENV_PYTHON" -m pip install --upgrade pip
"$VENV_PYTHON" -m pip install -e "${PROJECT_ROOT}[docs,polars]"

echo "Building packaged docs..."
"$VENV_PYTHON" "$PROJECT_ROOT/scripts/build_packaged_docs.py"

echo "Packaged docs build complete."

if [[ -t 0 ]]; then
  echo
  read "?Press return to close..."
fi
