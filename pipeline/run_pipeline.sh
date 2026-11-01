#!/usr/bin/env bash
# ==============================================================================
# Pharmacovigilance FAERS Pipeline Execution Wrapper (Linux / macOS / WSL)
# ==============================================================================

set -e

# Resolve script and project directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "================================================================================"
echo "    Launching Pharmacovigilance FAERS Big Data Analytics Pipeline               "
echo "================================================================================"
echo "Project Root : $PROJECT_ROOT"

# 1. Activate Python virtual environment if found
if [ -d "$PROJECT_ROOT/.venv" ]; then
    echo "[INFO] Activating virtual environment from .venv..."
    source "$PROJECT_ROOT/.venv/bin/activate"
elif [ -d "$PROJECT_ROOT/venv" ]; then
    echo "[INFO] Activating virtual environment from venv..."
    source "$PROJECT_ROOT/venv/bin/activate"
fi

# 2. Check and configure SPARK_HOME if not already set
if [ -z "$SPARK_HOME" ]; then
    # Attempt to locate Spark via python pyspark package
    PYSPARK_PATH=$(python -c "import pyspark; import os; print(os.path.dirname(pyspark.__file__))" 2>/dev/null || true)
    if [ -n "$PYSPARK_PATH" ] && [ -d "$PYSPARK_PATH" ]; then
        export SPARK_HOME="$PYSPARK_PATH"
        echo "[INFO] Detected PySpark SPARK_HOME: $SPARK_HOME"
    else
        echo "[WARNING] SPARK_HOME is not set. Relying on system PATH or PySpark bundle."
    fi
else
    echo "[INFO] Using existing SPARK_HOME: $SPARK_HOME"
fi

# 3. Add project root to PYTHONPATH
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# 4. Execute Full Pipeline Orchestrator with all passed arguments
echo "[INFO] Executing python pipeline/run_full_pipeline.py $@ ..."
python "$PROJECT_ROOT/pipeline/run_full_pipeline.py" "$@"

echo "================================================================================"
echo "    Pipeline Execution Completed Successfully                                   "
echo "================================================================================"
