#!/usr/bin/env bash
# =============================================================================
# Cardiac Imaging Intelligence Platform — Deployment Script
# Executes all SQL files in order against a Snowflake account.
# =============================================================================

set -euo pipefail

CONNECTION="${1:---connection}"
CONN_NAME=""

usage() {
    echo "Usage: $0 --connection <snowflake-connection-name> [--skip-data-load]"
    echo ""
    echo "Options:"
    echo "  --connection <name>   Snowflake connection name from ~/.snowflake/connections.toml"
    echo "  --skip-data-load      Skip the data load step (07_data_load.sql) — useful for re-running DDLs only"
    echo ""
    echo "Example:"
    echo "  $0 --connection spark-connect"
    exit 1
}

SKIP_LOAD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --connection)
            CONN_NAME="$2"
            shift 2
            ;;
        --skip-data-load)
            SKIP_LOAD=true
            shift
            ;;
        --help|-h)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

if [[ -z "$CONN_NAME" ]]; then
    echo "Error: --connection is required."
    usage
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="${SCRIPT_DIR}/sql"

if [[ ! -d "$SQL_DIR" ]]; then
    echo "Error: sql/ directory not found at ${SQL_DIR}"
    exit 1
fi

echo "============================================================"
echo "  Cardiac Imaging Intelligence Platform — Deployment"
echo "  Connection: ${CONN_NAME}"
echo "============================================================"
echo ""

run_sql_file() {
    local file=$1
    local desc=$2
    echo "━━━ [$(date +%H:%M:%S)] ${desc}"
    echo "    File: ${file}"
    if command -v snow &>/dev/null; then
        snow sql -f "${file}" --connection "${CONN_NAME}" 2>&1 | tail -5
    elif command -v snowsql &>/dev/null; then
        snowsql -c "${CONN_NAME}" -f "${file}" 2>&1 | tail -5
    else
        echo "    Error: Neither 'snow' CLI nor 'snowsql' found. Install Snowflake CLI."
        echo "    Alternatively, run the SQL files manually in Snowsight."
        exit 1
    fi
    echo ""
}

run_sql_file "${SQL_DIR}/01_database_schema.sql" "Creating database and schema..."
run_sql_file "${SQL_DIR}/02_stages.sql"          "Creating external stages and refreshing directories..."
run_sql_file "${SQL_DIR}/03_udfs.sql"            "Creating Python UDFs (DICOM metadata extraction, image rendering)..."
run_sql_file "${SQL_DIR}/04_tables.sql"          "Creating metadata and AI enrichment tables..."
run_sql_file "${SQL_DIR}/05_masking_policies.sql" "Creating and applying PHI masking policies..."
run_sql_file "${SQL_DIR}/06_cortex_search.sql"   "Creating Cortex Search Service..."

if [[ "$SKIP_LOAD" == true ]]; then
    echo "━━━ Skipping data load (--skip-data-load flag set)"
else
    echo "━━━ [$(date +%H:%M:%S)] Loading data (2,848 DICOM files — this may take 5-10 minutes)..."
    run_sql_file "${SQL_DIR}/07_data_load.sql" "Extracting DICOM metadata from binary files..."
fi

echo ""
echo "============================================================"
echo "  Deployment complete!"
echo ""
echo "  To launch the Streamlit app:"
echo "    pip install -r requirements.txt"
echo "    SNOWFLAKE_CONNECTION_NAME=${CONN_NAME} streamlit run app.py"
echo "============================================================"
