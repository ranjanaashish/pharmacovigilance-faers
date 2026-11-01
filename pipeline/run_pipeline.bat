@echo off
REM ==============================================================================
REM Pharmacovigilance FAERS Pipeline Execution Wrapper (Windows Command Prompt)
REM ==============================================================================

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
cd /d "%PROJECT_ROOT%"

echo ================================================================================
echo    Launching Pharmacovigilance FAERS Big Data Analytics Pipeline (Windows)
echo ================================================================================
echo Project Root: %CD%

REM 1. Activate virtual environment if present
if exist "%CD%\.venv\Scripts\activate.bat" (
    echo [INFO] Activating virtual environment from .venv...
    call "%CD%\.venv\Scripts\activate.bat"
) else if exist "%CD%\venv\Scripts\activate.bat" (
    echo [INFO] Activating virtual environment from venv...
    call "%CD%\venv\Scripts\activate.bat"
)

REM 2. Check and configure SPARK_HOME if not already set
if "%SPARK_HOME%"=="" (
    for /f "delims=" %%I in ('python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))" 2^>nul') do (
        set "SPARK_HOME=%%I"
    )
    if not "!SPARK_HOME!"=="" (
        echo [INFO] Detected PySpark SPARK_HOME: !SPARK_HOME!
    ) else (
        echo [WARNING] SPARK_HOME is not set. Using bundled PySpark.
    )
) else (
    echo [INFO] Using existing SPARK_HOME: %SPARK_HOME%
)

REM 3. Set PYTHONPATH to include project root
set "PYTHONPATH=%CD%;%PYTHONPATH%"

REM 4. Execute Full Pipeline Orchestrator with all arguments passed through
echo [INFO] Executing python pipeline\run_full_pipeline.py %* ...
python pipeline\run_full_pipeline.py %*

if %ERRORLEVEL% EQU 0 (
    echo ================================================================================
    echo    Pipeline Execution Completed Successfully
    echo ================================================================================
) else (
    echo ================================================================================
    echo    Pipeline Failed with Exit Code: %ERRORLEVEL%
    echo ================================================================================
)

exit /b %ERRORLEVEL%
