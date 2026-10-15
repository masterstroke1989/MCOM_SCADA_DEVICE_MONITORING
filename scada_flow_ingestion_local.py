#!/usr/bin/env python3
"""
SCADA Flow Data Ingestion: SQL Server -> Databricks Delta
=========================================================
High-performance bulk loading via Parquet + COPY INTO.

Run this script on your LOCAL machine (not in Databricks).
It reads SQL Server tables, writes them as Parquet files locally,
uploads them to a Unity Catalog Volume, and uses COPY INTO for
fast bulk ingestion into Delta tables.

Prerequisites:
  pip install pymssql databricks-sql-connector databricks-sdk pandas pyarrow

Usage:
  python scada_flow_ingestion_local.py
"""

import pymssql
import pandas as pd
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
import time
import sys
import os
import tempfile
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# CONFIGURATION
# ============================================================

# SQL Server (local)
SQL_SERVER_HOST = "localhost"
SQL_SERVER_PORT = 1433
SQL_SERVER_USER = os.environ.get("SQL_USER", "tushar89")
SQL_SERVER_PASS = os.environ.get("SQL_PASS", "")
DATABASES = ["SCADA_FLOW_2025", "SCADA_FLOW_2026"]

# Databricks
DATABRICKS_HOST = "https://dbc-526464cb-e1ae.cloud.databricks.com"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/401badd6bd71f05e"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

# Target catalog in Unity Catalog
TARGET_CATALOG = "workspace"

# Rows per Parquet chunk (500K rows per file balances size and parallelism)
CHUNK_SIZE = 500_000

# Parallel upload threads
UPLOAD_THREADS = 4


def get_sqlserver_connection(database):
    return pymssql.connect(
        server=SQL_SERVER_HOST,
        port=SQL_SERVER_PORT,
        user=SQL_SERVER_USER,
        password=SQL_SERVER_PASS,
        database=database,
    )


def get_databricks_connection():
    return dbsql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )


def get_databricks_sdk_client():
    return WorkspaceClient(
        host=DATABRICKS_HOST,
        token=DATABRICKS_TOKEN,
    )


def discover_nonempty_tables(database):
    conn = get_sqlserver_connection(database)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """)
    tables = cursor.fetchall()

    print(f"\n{'='*60}")
    print(f"Database: {database}")
    print(f"Total tables found: {len(tables)}")
    print(f"{'='*60}")

    nonempty = []
    for schema_name, table_name in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM [{schema_name}].[{table_name}]")
            row_count = cursor.fetchone()[0]
            if row_count > 0:
                nonempty.append((schema_name, table_name, row_count))
                print(f"  [INCLUDE] [{schema_name}].[{table_name}] -> {row_count:,} rows")
            else:
                print(f"  [SKIP]    [{schema_name}].[{table_name}] -> EMPTY")
        except Exception as e:
            print(f"  [ERROR]   [{schema_name}].[{table_name}] -> {e}")

    cursor.close()
    conn.close()
    return nonempty


def upload_file_to_volume(sdk_client, local_path, volume_path):
    """Upload a local file to a UC Volume using the Databricks SDK."""
    with open(local_path, "rb") as f:
        sdk_client.files.upload(volume_path, f, overwrite=True)


def load_table(database, src_schema, src_table, row_count):
    """
    High-performance table load:
    1. Read SQL Server data in chunks via pandas
    2. Write each chunk as a local Parquet file
    3. Upload Parquet files to UC Volume in parallel
    4. COPY INTO the Delta table from the volume
    """
    target_schema = database.lower()
    delta_name = src_table.lower().replace(" ", "_").replace("-", "_")
    full_target = f"{TARGET_CATALOG}.{target_schema}.{delta_name}"
    volume_base = f"/Volumes/{TARGET_CATALOG}/{target_schema}/staging/{delta_name}"
    source_ref = f"[{src_schema}].[{src_table}]"

    start = time.time()
    tmp_dir = tempfile.mkdtemp(prefix=f"scada_{delta_name}_")

    try:
        sdk_client = get_databricks_sdk_client()
        db_conn = get_databricks_connection()
        db_cursor = db_conn.cursor()

        # --- Phase 1: Read from SQL Server and write Parquet chunks ---
        print(f"\n  {source_ref} ({row_count:,} rows)")
        print(f"  Phase 1: Reading from SQL Server -> local Parquet files...")

        ss_conn = get_sqlserver_connection(database)
        select_cols = ", ".join(f"[{c}]" for c in get_column_names(database, src_schema, src_table))
        query = f"SELECT {select_cols} FROM [{src_schema}].[{src_table}]"

        chunk_files = []
        rows_read = 0
        chunk_idx = 0

        for chunk_df in pd.read_sql(query, ss_conn, chunksize=CHUNK_SIZE):
            # Sanitize column names
            chunk_df.columns = [
                c.replace(" ", "_").replace("-", "_") for c in chunk_df.columns
            ]

            # Write Parquet
            parquet_path = os.path.join(tmp_dir, f"chunk_{chunk_idx:04d}.parquet")
            chunk_df.to_parquet(parquet_path, engine="pyarrow", index=False)
            chunk_files.append(parquet_path)

            rows_read += len(chunk_df)
            chunk_idx += 1
            file_size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
            pct = min(100, int(rows_read / row_count * 100))
            print(f"    Chunk {chunk_idx}: {len(chunk_df):,} rows, "
                  f"{file_size_mb:.1f} MB  [{rows_read:,}/{row_count:,} = {pct}%]")

        ss_conn.close()
        elapsed_phase1 = time.time() - start
        total_size_mb = sum(os.path.getsize(f) / (1024*1024) for f in chunk_files)
        print(f"  Phase 1 done: {chunk_idx} files, {total_size_mb:.1f} MB total ({elapsed_phase1:.1f}s)")

        # --- Phase 2: Upload Parquet files to UC Volume in parallel ---
        print(f"  Phase 2: Uploading to volume {volume_base}/ ...")
        phase2_start = time.time()

        uploaded = 0
        def upload_one(local_file):
            fname = os.path.basename(local_file)
            vol_path = f"{volume_base}/{fname}"
            upload_file_to_volume(sdk_client, local_file, vol_path)
            return fname

        with ThreadPoolExecutor(max_workers=UPLOAD_THREADS) as executor:
            futures = {executor.submit(upload_one, f): f for f in chunk_files}
            for future in as_completed(futures):
                uploaded += 1
                fname = future.result()
                print(f"    Uploaded {uploaded}/{len(chunk_files)}: {fname}")

        elapsed_phase2 = time.time() - phase2_start
        print(f"  Phase 2 done: {uploaded} files uploaded ({elapsed_phase2:.1f}s)")

        # --- Phase 3: COPY INTO Delta table ---
        print(f"  Phase 3: COPY INTO {full_target} ...")
        phase3_start = time.time()

        # Drop and recreate to ensure clean load
        db_cursor.execute(f"DROP TABLE IF EXISTS {full_target}")

        copy_sql = f"""
            CREATE TABLE {full_target}
            USING DELTA
            AS SELECT * FROM read_files(
                '{volume_base}/',
                format => 'parquet',
                pathGlobFilter => '*.parquet'
            )
        """
        db_cursor.execute(copy_sql)

        elapsed_phase3 = time.time() - phase3_start
        print(f"  Phase 3 done: Delta table created ({elapsed_phase3:.1f}s)")

        # Verify row count
        db_cursor.execute(f"SELECT COUNT(*) FROM {full_target}")
        delta_count = db_cursor.fetchone()[0]

        db_cursor.close()
        db_conn.close()

        elapsed_total = time.time() - start
        status = "OK" if delta_count == rows_read else "WARN: row count mismatch"
        print(f"  [{status}] {full_target}: {delta_count:,} rows in Delta "
              f"(source: {rows_read:,}) | Total: {elapsed_total:.1f}s")

        return delta_count == rows_read

    except Exception as e:
        elapsed = time.time() - start
        print(f"\n  [FAIL] {source_ref} -> {e} ({elapsed:.1f}s)")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Clean up local temp files
        shutil.rmtree(tmp_dir, ignore_errors=True)


def get_column_names(database, schema_name, table_name):
    """Get column names in ordinal order."""
    conn = get_sqlserver_connection(database)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}'
          AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """)
    cols = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return cols


def cleanup_staging(database):
    """Remove uploaded Parquet files from the staging volume."""
    target_schema = database.lower()
    print(f"\n  Cleaning staging volume for {target_schema}...")
    try:
        db_conn = get_databricks_connection()
        db_cursor = db_conn.cursor()
        volume_path = f"/Volumes/{TARGET_CATALOG}/{target_schema}/staging"
        db_cursor.execute(f"LIST '{volume_path}'")
        files = db_cursor.fetchall()
        sdk_client = get_databricks_sdk_client()
        for f in files:
            file_path = f[0]
            if file_path.endswith(".parquet") or "/chunk_" in file_path:
                try:
                    sdk_client.files.delete(file_path)
                except Exception:
                    pass
        db_cursor.close()
        db_conn.close()
        print(f"  Staging cleaned.")
    except Exception as e:
        print(f"  Staging cleanup skipped: {e}")


def main():
    print("="*60)
    print("SCADA Flow Data Ingestion: SQL Server -> Databricks Delta")
    print("HIGH-PERFORMANCE MODE (Parquet + COPY INTO)")
    print("="*60)
    print(f"\nChunk size: {CHUNK_SIZE:,} rows per Parquet file")
    print(f"Upload threads: {UPLOAD_THREADS}")

    overall_start = time.time()

    # Step 1: Discover non-empty tables
    print("\n[Step 1] Discovering non-empty tables...")
    tables_to_load = {}
    for database in DATABASES:
        tables_to_load[database] = discover_nonempty_tables(database)

    total = sum(len(v) for v in tables_to_load.values())
    print(f"\nTotal tables to load: {total}")
    if total == 0:
        print("No non-empty tables found. Exiting.")
        sys.exit(0)

    # Step 2: Load tables
    print("\n[Step 2] Loading tables (Parquet -> Volume -> COPY INTO)...")
    results = {"success": 0, "failed": 0}
    for database in DATABASES:
        tables = tables_to_load[database]
        if not tables:
            print(f"\n  [SKIP] {database}: no non-empty tables.")
            continue
        print(f"\n{'='*60}")
        print(f"Loading {len(tables)} table(s) from {database}")
        print(f"{'='*60}")
        for src_schema, src_table, row_count in tables:
            ok = load_table(database, src_schema, src_table, row_count)
            results["success" if ok else "failed"] += 1

    # Step 3: Cleanup staging
    print("\n[Step 3] Cleaning up staging volumes...")
    for database in DATABASES:
        cleanup_staging(database)

    # Step 4: Summary
    overall_elapsed = time.time() - overall_start
    print(f"\n{'='*60}")
    print(f"INGESTION COMPLETE")
    print(f"  Succeeded: {results['success']}")
    print(f"  Failed:    {results['failed']}")
    print(f"  Total time: {overall_elapsed:.1f}s ({overall_elapsed/60:.1f} min)")
    print(f"{'='*60}")

    # Step 5: Verify
    print("\n[Step 5] Verifying loaded tables...")
    db_conn = get_databricks_connection()
    db_cursor = db_conn.cursor()
    for database in DATABASES:
        schema_name = database.lower()
        full_schema = f"{TARGET_CATALOG}.{schema_name}"
        print(f"\nSchema: {full_schema}")
        print("-" * 50)
        db_cursor.execute(f"SHOW TABLES IN {full_schema}")
        tables = db_cursor.fetchall()
        if not tables:
            print("  (no tables)")
        else:
            for t in tables:
                tbl_name = t[1]
                full_tbl = f"{full_schema}.{tbl_name}"
                db_cursor.execute(f"SELECT COUNT(*) FROM {full_tbl}")
                cnt = db_cursor.fetchone()[0]
                print(f"  {full_tbl} -> {cnt:,} rows")
    db_cursor.close()
    db_conn.close()


# ============================================================
# PIPELINE ORCHESTRATION: Trigger Databricks Job after ingestion
# ============================================================

DATA_PIPELINE_JOB_ID = 127178333405535  # SCADA Flow - Data Pipeline (24h refresh)

def trigger_databricks_pipeline():
    """Trigger the Databricks cleaning/validation Job after ingestion."""
    import requests
    import json

    run_id = f"ingestion_{int(time.time())}"
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "job_id": DATA_PIPELINE_JOB_ID,
        "notebook_params": {
            "run_id": run_id
        }
    }

    print(f"\n[Step 6] Triggering Databricks data pipeline (Job ID: {DATA_PIPELINE_JOB_ID})...")
    print(f"  Run ID: {run_id}")

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        job_run_id = result.get("run_id", "unknown")
        print(f"  Pipeline triggered successfully!")
        print(f"  Databricks Run ID: {job_run_id}")
        print(f"  Monitor at: {DATABRICKS_HOST}/jobs/{DATA_PIPELINE_JOB_ID}/runs/{job_run_id}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"  [WARN] Failed to trigger pipeline: {e}")
        print(f"  You can run the pipeline manually from the Databricks Jobs page.")
        return False


if __name__ == "__main__":
    main()

    # After successful ingestion, trigger the cleaning pipeline
    print("\n" + "="*60)
    print("ORCHESTRATION: Triggering data pipeline...")
    print("="*60)
    trigger_databricks_pipeline()
