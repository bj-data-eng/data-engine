"""Argument parser construction for the CLI surface."""

from __future__ import annotations

import argparse
from pathlib import Path

from data_engine.platform.identity import APP_DISPLAY_NAME, APP_DISTRIBUTION_NAME
from data_engine.ui.cli.commands_run import TEST_SLICE_CHOICES


class _HelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Readable CLI help with preserved examples."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=APP_DISTRIBUTION_NAME,
        description=f"{APP_DISPLAY_NAME} command-line interface.",
        epilog=(
            "Examples:\n"
            "  data-engine run gui\n"
            "  data-engine run egui\n"
            "  data-engine run tui\n"
            "  data-engine create workspace .\\workspaces\\docs\n"
            "  data-engine create workspace ./workspaces/docs\n"
            "  data-engine run tests\n"
            "  data-engine run tests all\n"
            "  data-engine doctor"
        ),
        formatter_class=_HelpFormatter,
    )
    parser.add_argument("--workspace", type=Path, help="Authored workspace root to use.")
    parser.add_argument("--app-root", type=Path, help=f"{APP_DISPLAY_NAME} project/app root used for local artifacts.")

    subparsers = parser.add_subparsers(dest="command", required=True, metavar="{start,create,run,doctor}")

    start_parser = subparsers.add_parser("start", help="Launch one Data Engine operator surface.")
    start_subparsers = start_parser.add_subparsers(dest="start_command", required=True, metavar="{gui,egui,tui}")
    start_subparsers.add_parser("gui", help="Launch the desktop GUI.")
    start_subparsers.add_parser("egui", help="Launch the experimental egui surface.")
    start_subparsers.add_parser("tui", help="Launch the terminal UI.")

    create_parser = subparsers.add_parser("create", help="Create Data Engine scaffolding.")
    create_subparsers = create_parser.add_subparsers(dest="create_command", required=True, metavar="{workspace}")
    workspace_parser = create_subparsers.add_parser("workspace", help="Create and select one authored workspace.")
    workspace_parser.add_argument("path", type=Path, help="Path to the workspace root to create.")

    run_parser = subparsers.add_parser("run", help="Run helpful project tasks or launch one operator surface.")
    run_subparsers = run_parser.add_subparsers(dest="run_command", required=True, metavar="{gui,egui,tui,tests}")
    run_subparsers.add_parser("gui", help="Launch the desktop GUI.")
    run_subparsers.add_parser("egui", help="Launch the experimental egui surface.")
    run_subparsers.add_parser("tui", help="Launch the terminal UI.")
    tests_parser = run_subparsers.add_parser("tests", help="Run one curated test slice.")
    tests_parser.add_argument("slice", nargs="?", default="unit", choices=TEST_SLICE_CHOICES, help="Named test slice to run.")
    tests_parser.add_argument("--list-slices", action="store_true", help="Print the available named test slices.")

    doctor_parser = subparsers.add_parser("doctor", help="Inspect the local Data Engine environment and workspace setup.")
    doctor_subparsers = doctor_parser.add_subparsers(dest="doctor_command", required=False, metavar="{daemons}")
    doctor_subparsers.add_parser("daemons", help="Inspect Data Engine daemon and related process state.")
    return parser

