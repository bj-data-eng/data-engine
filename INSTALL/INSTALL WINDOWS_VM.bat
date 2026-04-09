@echo off
setlocal

for %%I in ("%~f0") do set "SCRIPT_DIR=%%~dpI"
for %%I in ("%SCRIPT_DIR%..") do set "PROJECT_ROOT=%%~fI"
set "VENV_DIR=%PROJECT_ROOT%\.venv"
set "VENV_PYTHON=%VENV_DIR%\Scripts\python.exe"

if not exist "%PROJECT_ROOT%\pyproject.toml" (
  echo Could not locate pyproject.toml next to the installer.
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

echo Using Python launcher: %PYTHON_CMD%
echo Project root: %PROJECT_ROOT%
echo Virtual environment: %VENV_DIR%

if not exist "%VENV_PYTHON%" (
  echo.
  echo Creating virtual environment...
  %PYTHON_CMD% -m venv "%VENV_DIR%"
  if errorlevel 1 (
    echo.
    echo Virtual environment creation failed.
    echo.
    pause
    exit /b 1
  )
)

echo.
echo Upgrading pip...
"%VENV_PYTHON%" -m pip install --upgrade pip
if errorlevel 1 (
  echo.
  echo pip upgrade failed.
  echo.
  pause
  exit /b 1
)

echo.
echo Installing Data Engine with dev extras and Polars LTS CPU...
"%VENV_PYTHON%" -m pip install -e "%PROJECT_ROOT%[dev,polars-lts]"
if errorlevel 1 (
  echo.
  echo Installation failed.
  echo.
  pause
  exit /b 1
)

echo.
echo Install complete.
echo Launch with: "%VENV_PYTHON%" -m data_engine.ui.cli.app start gui

pause
popd
exit /b 0
