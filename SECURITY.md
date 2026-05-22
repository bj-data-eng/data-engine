# Security

This project is maintained with dependency pinning, constrained installs, and
explicit audit checks so local development and release builds do not silently
float to newly published dependency versions.

## Supported Versions

Security fixes are applied to the current development line. Older local builds
should be refreshed from the latest repository state before running production
or work data.

## Reporting A Vulnerability

If this repository is published on GitHub, report vulnerabilities through
GitHub's private vulnerability reporting when it is enabled for the repository.
For private development, report security issues directly to the project
maintainer and avoid posting exploit details in issues, logs, screenshots, or
public comments.

## Dependency Controls

Data Engine uses several layers of dependency control:

- Direct project dependencies are pinned in `pyproject.toml`.
- Development and installer scripts apply `requirements/constraints.txt` to
  constrain transitive dependencies.
- Runtime-only Windows Python 3.14 installs can use
  `requirements/locked-runtime-win-py314.txt` with `--require-hashes`.
- Installer scripts pass the constraints file during editable installs so the
  local environment resolves the same reviewed dependency set.
- Build isolation dependencies are pinned because the build process generates
  the packaged Sphinx documentation.

Use constrained installs for normal local work:

```powershell
.\.venv\Scripts\python.exe -m pip install --constraint requirements\constraints.txt -e ".[dev]"
```

Use the hash-locked runtime file when installing only the reviewed Windows
runtime dependency set:

```powershell
python -m pip install --require-hashes -r requirements\locked-runtime-win-py314.txt
```

## Audit Checklist

Run these checks before accepting dependency changes:

```powershell
.\.venv\Scripts\python.exe -m pip_audit --local --progress-spinner off
.\.venv\Scripts\python.exe -m pip_audit -r requirements\constraints.txt --progress-spinner off
.\.venv\Scripts\python.exe -m pip_audit -r requirements\locked-runtime-win-py314.txt --progress-spinner off
.\.venv\Scripts\python.exe -m pip check
```

For packaging changes, also verify the project still builds cleanly:

```powershell
.\.venv\Scripts\python.exe -m build
.\.venv\Scripts\python.exe -m twine check dist\*
```

## Refreshing Pinned Dependencies

Refresh dependencies intentionally:

1. Review the package purpose, release notes, PyPI project metadata, and yanked
   file status for each changed package.
2. Update direct pins in `pyproject.toml` when author-facing dependency
   requirements change.
3. Update `requirements/constraints.txt` for reviewed transitive dependencies.
4. Regenerate hash-locked runtime files only for the target environments they
   describe.
5. Run the audit checklist and focused tests for affected functionality.
6. Run the full packaging check before release.

The hash-locked runtime file is target-specific. Do not reuse
`requirements/locked-runtime-win-py314.txt` for macOS, Linux, or another Python
minor version.

## Local Artifacts

Keep local work data, generated workspaces, wheelhouses, and audit scratch files
out of commits. Use ignored paths such as `workspaces/_local/` for local notes
or generated artifacts that should survive between sessions without becoming
repository content.
