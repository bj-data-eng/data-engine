"""Sphinx configuration for the Data Engine project docs."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))

project = "Data Engine"
author = "Data Engine contributors"
release = "0.1.2"

extensions = [
    "sphinx.ext.autodoc",
    "myst_parser",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_title = "Data Engine documentation"
autodoc_member_order = "bysource"
autodoc_typehints = "description"
add_module_names = False
