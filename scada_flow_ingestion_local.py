#!/usr/bin/env python3
"""
SCADA Flow Data Ingestion: SQL Server -> Databricks Delta
=========================================================
mcom technology - SCADA Flow Monitoring Project

High-performance bulk loading via Parquet + read_files().

Run this script on your LOCAL machine (not in Databricks).
Prerequisites:
  pip install pymssql databricks-sql-connector databricks-sdk pandas pyarrow

Usage:
  python scada_flow_ingestion_local.py

(c) 2026 mcom technology. All rights reserved.
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

SQL_SERVER_HOST = "localhost"
SQL_SERVER_PORT = 1433
SQL_SERVER_USER = os.environ.get("SQL_USER", "<your_sql_user>")
SQL_SERVER_PASS = os.environ.get("SQL_PASS", "<your_sql_password>")
DATABASES = ["SCADA_FLOW_2025", "SCADA_FLOW_2026"]

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://dbc-526464cb-e1ae.cloud.databricks.com")
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/401badd6bd71f05e"
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "<YOUR_TOKEN_HERE>")

TARGET_CATALOG = "workspace"
CHUNK_SIZE = 500_000
UPLOAD_THREADS = 4


def get_sqlserver_connection(database):
    return pymssql.connect(
        server=SQL_SERVER_HOST, port=SQL_SERVER_PORT,
        user=SQL_SERVER_USER, password=SQL_SERVER_PASS,
        database=database,
    )


def get_databricks_connection():
    return dbsql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )


def get_databricks_sdk_client():
    return WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)


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
    print(f"Database: {database} | Tables found: {len(tables)}")
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
    with open(local_path, "rb") as f:
        sdk_client.files.upload(volume_path, f, overwrite=True)


def get_column_names(database, schema_name, table_name):
    conn = get_sqlserver_connection(database)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """)
    cols = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return cols


def load_table(database, src_schema, src_table, row_count):
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

        # Phase 1: Read SQL Server -> local Parquet
        print(f"\n  {source_ref} ({row_count:,} rows)")
        print(f"  Phase 1: Reading -> local Parquet files...")
        ss_conn = get_sqlserver_connection(database)
        select_cols = ", ".join(f"[{c}]" for c in get_column_names(database, src_schema, src_table))
        query = f"SELECT {select_cols} FROM [{src_schema}].[{src_table}]"
        chunk_files = []
        rows_read = 0
        chunk_idx = 0
        for chunk_df in pd.read_sql(query, ss_conn, chunksize=CHUNK_SIZE):
            chunk_df.columns = [c.replace(" ", "_").replace("-", "_") for c in chunk_df.columns]
            parquet_path = os.path.join(tmp_dir, f"chunk_{chunk_idx:04d}.parquet")
            chunk_df.to_parquet(parquet_path, engine="pyarrow", index=False)
            chunk_files.append(parquet_path)
            rows_read += len(chunk_df)
            chunk_idx += 1
            file_size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
            pct = min(100, int(rows_read / row_count * 100))
            print(f"    Chunk {chunk_idx}: {len(chunk_df):,} rows, {file_size_mb:.1f} MB [{pct}%]")
        ss_conn.close()
        print(f"  Phase 1 done: {chunk_idx} files ({time.time()-start:.1f}s)")

        # Phase 2: Upload to UC Volume in parallel
        print(f"  Phase 2: Uploading to {volume_base}/ ...")
        phase2_start = time.time()
        uploaded = 0
        def upload_one(local_file):
            fname = os.path.basename(local_file)
            upload_file_to_volume(sdk_client, local_file, f"{volume_base}/{fname}")
            return fname
        with ThreadPoolExecutor(max_workers=UPLOAD_THREADS) as executor:
            futures = {executor.submit(upload_one, f): f for f in chunk_files}
            for future in as_completed(futures):
                uploaded += 1
                print(f"    Uploaded {uploaded}/{len(chunk_files)}: {future.result()}")
        print(f"  Phase 2 done ({time.time()-phase2_start:.1f}s)")

        # Phase 3: Create Delta table
        print(f"  Phase 3: CREATE TABLE {full_target} ...")
        db_cursor.execute(f"DROP TABLE IF EXISTS {full_target}")
        db_cursor.execute(f"""
            CREATE TABLE {full_target} USING DELTA
            AS SELECT * FROM read_files('{volume_base}/',
                format => 'parquet', pathGlobFilter => '*.parquet')
        """)
        db_cursor.execute(f"SELECT COUNT(*) FROM {full_target}")
        delta_count = db_cursor.fetchone()[0]
        db_cursor.close()
        db_conn.close()
        status = "OK" if delta_count == rows_read else "WARN: mismatch"
        print(f"  [{status}] {full_target}: {delta_count:,} rows ({time.time()-start:.1f}s)")
        return delta_count == rows_read
    except Exception as e:
        print(f"  [FAIL] {source_ref} -> {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def main():
    print("="*60)
    print("SCADA Flow Data Ingestion: SQL Server -> Databricks Delta")
    print("mcom technology - HIGH-PERFORMANCE MODE (Parquet + read_files)")
    print("="*60)
    overall_start = time.time()

    tables_to_load = {}
    for database in DATABASES:
        tables_to_load[database] = discover_nonempty_tables(database)
    total = sum(len(v) for v in tables_to_load.values())
    print(f"\nTotal tables to load: {total}")
    if total == 0:
        print("No non-empty tables found. Exiting.")
        sys.exit(0)

    results = {"success": 0, "failed": 0}
    for database in DATABASES:
        for src_schema, src_table, row_count in tables_to_load.get(database, []):
            ok = load_table(database, src_schema, src_table, row_count)
            results["success" if ok else "failed"] += 1

    elapsed = time.time() - overall_start
    print(f"\n{'='*60}")
    print(f"COMPLETE: {results['success']} succeeded, {results['failed']} failed ({elapsed:.0f}s)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
