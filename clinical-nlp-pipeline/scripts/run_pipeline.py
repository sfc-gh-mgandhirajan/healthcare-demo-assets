#!/usr/bin/env python3
"""
Clinical NLP Pipeline Runner
Deploys stored procedures and runs the pipeline with progress reporting.
"""
import os
import sys
import time
import snowflake.connector
from pathlib import Path

CONN_NAME = os.getenv("SNOWFLAKE_CONNECTION_NAME", "polaris1")
SQL_DIR = Path(__file__).parent.parent / "sql"
SCHEMA = "UNSTRUCTURED_HEALTHDATA.CLINICAL_NLP"


def get_connection():
    return snowflake.connector.connect(connection_name=CONN_NAME)


def run_sql_file(conn, filepath):
    text = filepath.read_text()
    statements = [s.strip() for s in text.split(";") if s.strip()]
    cur = conn.cursor()
    for stmt in statements:
        if not stmt or stmt.startswith("--"):
            continue
        try:
            cur.execute(stmt)
        except Exception as e:
            print(f"  WARN: {e}")
    cur.close()


def deploy_objects(conn):
    print("=" * 60)
    print("DEPLOYING PIPELINE OBJECTS")
    print("=" * 60)

    folders = sorted(SQL_DIR.iterdir())
    for folder in folders:
        if not folder.is_dir():
            continue
        files = sorted(folder.glob("*.sql"))
        for f in files:
            print(f"  Deploying {folder.name}/{f.name}...")
            run_sql_file(conn, f)
    print("  All objects deployed.\n")


def run_pipeline(conn, batch_size=500, sample_limit=None):
    cur = conn.cursor()
    sample_arg = f", {sample_limit}" if sample_limit else ", NULL"

    print("=" * 60)
    print(f"RUNNING PIPELINE (batch_size={batch_size}, sample_limit={sample_limit})")
    print("=" * 60)

    cur.execute(f"CALL {SCHEMA}.SP_RUN_PIPELINE({batch_size}{sample_arg})")
    result = cur.fetchone()[0]
    print(f"\n  Result: {result}\n")
    cur.close()


def show_progress(conn):
    cur = conn.cursor()
    cur.execute(f"""
        SELECT step_name, status, rows_inserted, elapsed_seconds, batch_number
        FROM {SCHEMA}.PIPELINE_PROGRESS
        WHERE run_id = (SELECT MAX(run_id) FROM {SCHEMA}.PIPELINE_RUN_LOG WHERE step_name = 'PIPELINE_MASTER')
        ORDER BY step_order, batch_number NULLS FIRST
    """)
    rows = cur.fetchall()
    print("\n  PIPELINE PROGRESS:")
    print("  " + "-" * 80)
    print(f"  {'STEP':<35} {'STATUS':<12} {'ROWS':<10} {'TIME(s)':<10} {'BATCH'}")
    print("  " + "-" * 80)
    for r in rows:
        batch = r[4] if r[4] else ""
        print(f"  {r[0]:<35} {r[1]:<12} {r[2] or '':<10} {r[3] or '':<10} {batch}")
    cur.close()


def show_entity_counts(conn):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {SCHEMA}.ENTITY_COUNTS ORDER BY entity_type")
    rows = cur.fetchall()
    print("\n  ENTITY COUNTS:")
    print("  " + "-" * 60)
    print(f"  {'ENTITY TYPE':<30} {'TOTAL':<10} {'CODED':<10} {'CODED %'}")
    print("  " + "-" * 60)
    for r in rows:
        print(f"  {r[0]:<30} {r[1]:<10} {r[2] or '':<10} {r[3] or ''}%")
    cur.close()


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "help"

    if action == "deploy":
        conn = get_connection()
        deploy_objects(conn)
        conn.close()

    elif action == "sample":
        conn = get_connection()
        deploy_objects(conn)
        run_pipeline(conn, batch_size=20, sample_limit=20)
        show_progress(conn)
        show_entity_counts(conn)
        conn.close()

    elif action == "run":
        batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 500
        conn = get_connection()
        run_pipeline(conn, batch_size=batch_size)
        show_progress(conn)
        show_entity_counts(conn)
        conn.close()

    elif action == "progress":
        conn = get_connection()
        show_progress(conn)
        show_entity_counts(conn)
        conn.close()

    else:
        print("""
Clinical NLP Pipeline Runner

Usage:
  python run_pipeline.py deploy       # Deploy all SQL objects (no data processing)
  python run_pipeline.py sample       # Deploy + run on 20-doc sample
  python run_pipeline.py run [batch]  # Run full pipeline (default batch=500)
  python run_pipeline.py progress     # Show latest pipeline progress
        """)
