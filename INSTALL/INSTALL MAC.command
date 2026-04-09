#!/bin/zsh

set -euo pipefail

SCRIPT_PATH="${0:A}"
PROJECT_ROOT="$(cd "$(dirname "$SCRIPT_PATH")/.." && pwd -P)"
VENV_DIR="$PROJECT_ROOT/.venv"
VENV_PYTHON="$VENV_DIR/bin/python"

if [[ ! -f "$PROJECT_ROOT/pyproject.toml" ]]; then
  echo "Could not locate pyproject.toml next to the installer."
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

echo "Using Python at: $PYTHON_BIN"
echo "Project root: $PROJECT_ROOT"
echo "Virtual environment: $VENV_DIR"

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo
  echo "Creating virtual environment..."
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

echo
echo "Upgrading pip..."
"$VENV_PYTHON" -m pip install --upgrade pip

echo
echo "Installing Data Engine with dev extras..."
"$VENV_PYTHON" -m pip install -e "${PROJECT_ROOT}[dev,polars]"

echo
echo "Install complete."
echo "Launch with: $VENV_PYTHON -m data_engine.ui.cli.app start gui"

if [[ -t 0 ]]; then
  echo
  read "?Press return to close..."
fi
