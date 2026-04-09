@echo off
setlocal

for %%I in ("%~f0") do set "SCRIPT_DIR=%%~dpI"
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_ROOT=%%~fI"
set "VENV_DIR=%PROJECT_ROOT%\.venv"
set "VENV_PYTHON=%VENV_DIR%\Scripts\python.exe"

if not exist "%PROJECT_ROOT%\pyproject.toml" (
  echo Could not locate pyproject.toml next to the docs builder.
  echo Expected project root: %PROJECT_ROOT%
  echo.
  pause
  exit /b 1
)

pushd "%PROJECT_ROOT%"

set "PYTHON_CMD="
where py >nul 2>nul
if not errorlevel 1 (
  set "PYTHON_CMD=py -3"
)

if not defined PYTHON_CMD (
  where python >nul 2>nul
  if not errorlevel 1 (
    set "PYTHON_CMD=python"
  )
)

if not defined PYTHON_CMD (
  echo Python was not found on PATH.
  echo.
  pause
  exit /b 1
)

if not exist "%VENV_PYTHON%" (
  echo Creating virtual environment...
  %PYTHON_CMD% -m venv "%VENV_DIR%"
  if errorlevel 1 (
    echo Virtual environment creation failed.
    echo.
    pause
    exit /b 1
  )
)

echo Installing docs build dependencies...
"%VENV_PYTHON%" -m pip install --upgrade pip
if errorlevel 1 (
  echo pip upgrade failed.
  echo.
  pause
  exit /b 1
)

"%VENV_PYTHON%" -m pip install -e "%PROJECT_ROOT%[docs,polars]"
if errorlevel 1 (
  echo Dependency installation failed.
  echo.
  pause
  exit /b 1
)

echo Building packaged docs...
"%VENV_PYTHON%" "%PROJECT_ROOT%\scripts\build_packaged_docs.py"
if errorlevel 1 (
  echo Docs build failed.
  echo.
  pause
  exit /b 1
)

echo Packaged docs build complete.
pause
popd
exit /b 0
