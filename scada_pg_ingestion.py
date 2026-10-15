#!/usr/bin/env python3
"""
SCADA Flow Data Ingestion to PostgreSQL
========================================
Reads real-time SCADA device data from SQL Server (source) and syncs to
PostgreSQL (target) every 15 minutes. Supports both SQL Server direct
mode and HTTP endpoint fallback.

Usage:
    python scada_pg_ingestion.py              # Run once (SQL Server mode)
    python scada_pg_ingestion.py --daemon     # Run continuously every 15 minutes
    python scada_pg_ingestion.py --init-db    # Initialize PostgreSQL tables + seed IMEIs
    python scada_pg_ingestion.py --health     # Show health check / ingestion stats
    python scada_pg_ingestion.py --test 862360078612541  # Debug single IMEI
    python scada_pg_ingestion.py --scan       # Scan which IMEIs have data in SQL Server

Requirements:
    pip install psycopg2-binary pymssql requests databricks-sql-connector

Author: mcom technology
Date: 2026-10-15
"""

import argparse
import logging
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Optional

import re

import pymssql
import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── Configuration ───────────────────────────────────────────────────────────

# ── PostgreSQL Target ──
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "supply_chain",
    "user": "postgres",
    "password": "Mukta#1989",
}

# ── SQL Server Source (gamma.php writes here) ──
# Uses Windows Authentication by default. Set user/password for SQL auth.
# NOTE: pymssql can't resolve named instances (\SQLEXPRESS) without
# SQL Server Browser service. Use "localhost" with the explicit port instead.
# Find your port: SQL Server Configuration Manager → Protocols for SQLEXPRESS
# → TCP/IP → IP Addresses → IPAll → TCP Dynamic Ports (or TCP Port)
MSSQL_CONFIG = {
    "server": "localhost",            # Use localhost instead of named instance
    "port": 1433,                     # <-- UPDATE with your actual SQL Server port
    "database": "SCADA_FLOW_2026",
    "user": "tushar89",               # SQL Server username
    "password": "Mukta#1989",         # <-- UPDATE with your SQL Server password
}

# ── HTTP Fallback (if SQL Server unavailable) ──
DATA_CHECK_URL = "http://103.96.43.158/scada-flow/data-check"

POLL_INTERVAL_SECONDS = 15 * 60   # 15 minutes
MAX_WORKERS = 20                  # Concurrent HTTP requests
HTTP_TIMEOUT = 30                 # Seconds per request
TIMEZONE = "Asia/Kolkata"

# ── Databricks Push (for monitoring from Databricks) ──
# Serverless can't reach localhost, so we push stats TO Databricks instead.
DBX_CONFIG = {
    "host": "dbc-526464cb-e1ae.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/401badd6bd71f05e",
    "token": "",  # Leave empty — uses env var DATABRICKS_TOKEN instead
    "catalog": "workspace",
    "schema": "scada_flow_monitoring",
}
DBX_PUSH_ENABLED = True  # Pushes stats to Databricks after each cycle

# ─── Logging ─────────────────────────────────────────────────────────────────

log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "scada_ingestion.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("scada_pg")

# ─── Graceful Shutdown ───────────────────────────────────────────────────────

_shutdown = False

def _handle_signal(signum, frame):
    global _shutdown
    logger.info("Shutdown signal received. Finishing current cycle...")
    _shutdown = True

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ─── HTTP Session with Retry ────────────────────────────────────────────────

def _build_http_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# ─── Database Helpers ────────────────────────────────────────────────────────

def get_pg_connection():
    """Create a new PostgreSQL connection."""
    return psycopg2.connect(**PG_CONFIG)


def get_mssql_connection():
    """Create a new SQL Server connection."""
    return pymssql.connect(
        server=MSSQL_CONFIG["server"],
        port=MSSQL_CONFIG.get("port", 1433),
        database=MSSQL_CONFIG["database"],
        user=MSSQL_CONFIG.get("user"),
        password=MSSQL_CONFIG.get("password"),
        charset="utf8",
    )


def init_database():
    """Create all required tables if they don't exist."""
    ddl = """
    -- Active devices to poll
    CREATE TABLE IF NOT EXISTS tb_active_devices (
        imei            VARCHAR(20) PRIMARY KEY,
        device_name     VARCHAR(200),
        short_name      VARCHAR(100),
        site_code       VARCHAR(50),
        service_expiry  DATE,
        status          VARCHAR(20) DEFAULT 'Active',
        created_at      TIMESTAMP DEFAULT NOW()
    );

    -- Raw data strings (append-only log)
    CREATE TABLE IF NOT EXISTS tb_row_data (
        id              BIGSERIAL PRIMARY KEY,
        imei_number     VARCHAR(20) NOT NULL,
        raw_data        TEXT NOT NULL,
        server_dt       TIMESTAMP NOT NULL,
        server_d        DATE NOT NULL,
        url_name        VARCHAR(50) DEFAULT 'scada_pg_ingestion.py'
    );
    CREATE INDEX IF NOT EXISTS idx_row_data_imei ON tb_row_data (imei_number);
    CREATE INDEX IF NOT EXISTS idx_row_data_dt ON tb_row_data (server_dt);

    -- Live data (one row per IMEI, upserted)
    CREATE TABLE IF NOT EXISTS tb_livedata (
        imei            VARCHAR(20) PRIMARY KEY,
        way             VARCHAR(5),
        data_stamp      TIMESTAMP,
        p1  NUMERIC(14,2) DEFAULT 0, p2  NUMERIC(14,2) DEFAULT 0,
        p3  NUMERIC(14,2) DEFAULT 0, p4  NUMERIC(14,2) DEFAULT 0,
        p5  NUMERIC(14,2) DEFAULT 0, p6  NUMERIC(14,2) DEFAULT 0,
        p7  NUMERIC(14,2) DEFAULT 0, p8  NUMERIC(14,2) DEFAULT 0,
        p9  NUMERIC(14,2) DEFAULT 0, p10 NUMERIC(14,2) DEFAULT 0,
        p11 NUMERIC(14,2) DEFAULT 0, p12 NUMERIC(14,2) DEFAULT 0,
        p13 NUMERIC(14,2) DEFAULT 0, p14 NUMERIC(14,2) DEFAULT 0,
        p15 NUMERIC(14,2) DEFAULT 0, p16 NUMERIC(14,2) DEFAULT 0,
        p17 NUMERIC(14,2) DEFAULT 0, p18 NUMERIC(14,2) DEFAULT 0,
        p19 NUMERIC(14,2) DEFAULT 0, p20 NUMERIC(14,2) DEFAULT 0,
        p21 NUMERIC(14,2) DEFAULT 0, p22 NUMERIC(14,2) DEFAULT 0,
        p23 NUMERIC(14,2) DEFAULT 0, p24 NUMERIC(14,2) DEFAULT 0,
        p25 NUMERIC(14,2) DEFAULT 0, p26 NUMERIC(14,2) DEFAULT 0,
        p27 NUMERIC(14,2) DEFAULT 0, p28 NUMERIC(14,2) DEFAULT 0,
        p29 NUMERIC(14,2) DEFAULT 0, p30 NUMERIC(14,2) DEFAULT 0,
        update_status   VARCHAR(10) DEFAULT 'NEW',
        server_dt       TIMESTAMP,
        server_dt_2     TIMESTAMP,
        url_name        VARCHAR(50) DEFAULT 'scada_pg_ingestion.py',
        cmd_value       VARCHAR(100) DEFAULT 'N/A',
        csq             VARCHAR(10),
        other_1         VARCHAR(10) DEFAULT 'B',
        other_2         VARCHAR(10) DEFAULT 'B'
    );

    -- Ingestion run log (for monitoring)
    CREATE TABLE IF NOT EXISTS tb_ingestion_log (
        id              BIGSERIAL PRIMARY KEY,
        run_start       TIMESTAMP NOT NULL,
        run_end         TIMESTAMP,
        total_imeis     INT DEFAULT 0,
        success_count   INT DEFAULT 0,
        error_count     INT DEFAULT 0,
        no_data_count   INT DEFAULT 0,
        duration_sec    NUMERIC(10,2),
        status          VARCHAR(20) DEFAULT 'RUNNING'
    );
    """
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
        logger.info("PostgreSQL tables initialized successfully.")
    finally:
        conn.close()


def load_active_imeis(conn) -> list:
    """Load active IMEI numbers from tb_active_devices."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT imei FROM tb_active_devices 
            WHERE status = 'Active' 
              AND (service_expiry IS NULL OR service_expiry >= CURRENT_DATE)
            ORDER BY imei
        """)
        return [row[0] for row in cur.fetchall()]


def seed_active_devices():
    """Seed tb_active_devices with the 326 active, non-expired IMEI numbers
    from the Databricks workspace.scada_cleaned.device_list table.
    Only numeric IMEIs (proper 15-digit) are included."""
    
    ACTIVE_IMEIS = [
        "861190059453776", "861190059533064", "861190059565561", "861190059574142",
        "861190059574324", "861190059574415", "861190059575693", "861190059747714",
        "861190059771540", "861190059776952", "861190059776986", "861190059871399",
        "861190059886504", "861190059917143", "861190059917440", "861190059918273",
        "861190059919495", "861190059925369", "861190059926367", "861198060728276",
        "861198060730884", "861198060733706", "861198060734498", "861198060734647",
        "861198060735750", "861198060735891", "861198060735933", "861198063943906",
        "861198063944045", "861198063944128", "861198063944409", "861198063944425",
        "861198063944664", "861198063944680", "861198063944748", "861198063944995",
        "861198063945075", "861198063945612", "861198063946073", "861198063946164",
        "861198063946230", "861198063946297", "861198063946347", "861198063946388",
        "861198063946446", "861198063946529", "861198063946594", "861198063946610",
        "861198063946628", "861198063947006", "861198063947451", "861198063947907",
        "861198063947972", "861198063948004", "861198063948152", "861198063948269",
        "861198063948426", "861198063948996", "861198063949101", "861198063949168",
        "861198063949259", "861198063949309", "861198063950471", "861198063950505",
        "861198063950646", "861198063950778", "861198063950943", "861198063951214",
        "861198063952006", "861198063952014", "861198063952311", "861198063952378",
        "861198063952634", "861198063952667", "861198063952675", "861198063952691",
        "861198063952808", "861198063952832", "861198063952899", "861198063953095",
        "861198063953129", "861198063953475", "861198063953483", "861198063953525",
        "861198063953558", "861198063953814", "861198063953848", "861198063953988",
        "861198063954242", "861198063954499", "861198063955280", "861198063955298",
        "861198063955439", "861198063955496", "861198063955744", "861198063955769",
        "861198063955785", "861198063956031", "861198063956106", "861198063956478",
        "861198063956502", "861198063957344", "861198063959084", "861198063959308",
        "861198066251760", "861198066251778", "861198066251786", "861198066251810",
        "861198066252263", "861198066252818", "861198066254517", "861198066254533",
        "861198066255555", "861198066256561", "861198066257007", "861198066257999",
        "861198066258005", "861198066258294", "861198066258351", "861198066258476",
        "861198066258526", "861198066258617", "861198066258625", "861198066258740",
        "861198066258757", "861198066258849", "861198066258872", "861198066258898",
        "861198066259151", "861198066259227", "861198066259243", "861198066259268",
        "861198066259896", "861198066261314", "861198066261413", "861198066261462",
        "861198066261520", "861198066261629", "861198066261694", "861198066261744",
        "861198066261777", "861198066261793", "861198066261835", "861198066261843",
        "861198066261850", "861198066262049", "861198066262098", "861198066262148",
        "861198066262171", "861198066262205", "861198066262288", "861198066262445",
        "861198066262999", "861198066266560", "861198066266628", "861198066267832",
        "861198066267873", "861198066272980", "861198066276163", "861198066276189",
        "861198066276403", "861198066276486", "861198066277302", "861198066277435",
        "861198066277773", "861198066277922", "861198066277997", "861198066278037",
        "861198066278458", "861198066278516", "861198066278797", "861198066279647",
        "861198066279894", "861198066279902", "861198066279910", "861198066279928",
        "861198066280132", "861198066280736", "861198066280801", "861198066376244",
        "861198066846409", "862211074142191", "862211074164963", "862211074182338",
        "862360073239134", "862360073391497", "862360073400934", "862360073402039",
        "862360073419520", "862360073434776", "862360073440195", "862360073440484",
        "862360073822798", "862360074187928", "862360074190351", "862360074192019",
        "862360074193108", "862360078462368", "862360078591075", "862360078612541",
        "862360078613556", "862360078633471", "862360078693954", "862360078694135",
        "862360078694853", "862360078695256", "862360078695553", "862360078695660",
        "862360078695678", "862360078695934", "862360078696296", "862360078699449",
        "862360078699811", "862360078715385", "862360078757742", "862360078760605",
        "862360078777104", "862360079768540", "862360079769233", "862360079774100",
        "862360079776162", "862360079803883", "862360079803891", "862360079809542",
        "862360079812728", "862360079812785", "862360079815820", "862360079862046",
        "862360079901000", "862360079906819", "862407081294534", "862407081305991",
        "862407081335535", "862407081337689", "862407081337838", "862407081337945",
        "862407081338117", "862407081360087", "862407081376208", "862407081376380",
        "863597073738354", "863597073740616", "863597073741168", "863597073741200",
        "863597073741234", "863597073741267", "863597073741374", "863597073741408",
        "863597073741580", "863597073741606", "863597073742893", "863597073742927",
        "863597073742935", "863597073743263", "863597073743289", "863597073743578",
        "863597073743628", "863597073743651", "863597073745276", "863597073745375",
        "863597073745466", "863597073745474", "863597073745490", "863597073745516",
        "863597073745649", "863597073745706", "863597073745839", "863597073746886",
        "863597073747033", "863597073747462", "863597073747488", "863597073747561",
        "863597073749849", "863597073749922", "863597073750177", "863597073750342",
        "866340057564000", "866340057564059", "867542059641343", "867542059699556",
        "867542059699721", "867542059699754", "867542059700347", "867542059700610",
        "868618057038317", "868618059374116", "868618059384073", "868618059396374",
        "868618059458240", "868618059461947", "868626044430034", "868626044510710",
        "868626044699950", "868768062056992", "868768062060705", "868768062062578",
        "868768062067700", "868768062069391", "868768062070795", "868768066382626",
        "868768066382675", "868768066382899", "868768066383707", "868768066383715",
        "868768066383889", "868768066384242", "868768068058596", "868768068059107",
        "868768068059883", "868768068064461", "868768068064479", "868768068064487",
        "868768068064636", "868768068065609", "868768068066466", "868768068066532",
        "868768068066789", "868768068066854", "868768068067126", "868768068067753",
        "868768068068702", "868768068070252", "868768068070518", "868768068070534",
        "869622057866551", "869622058058737",
    ]
    
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            insert_sql = """
                INSERT INTO tb_active_devices (imei, status)
                VALUES (%s, 'Active')
                ON CONFLICT (imei) DO UPDATE SET status = 'Active'
            """
            psycopg2.extras.execute_batch(cur, insert_sql, [(imei,) for imei in ACTIVE_IMEIS])
        conn.commit()
        logger.info(f"Seeded {len(ACTIVE_IMEIS)} active IMEI numbers into tb_active_devices.")
    finally:
        conn.close()


# ─── Data Parsing (mirrors gamma.php logic) ──────────────────────────────────

def safe_float(value: str, decimals: int = 2) -> Optional[Decimal]:
    """Convert string to Decimal with fixed precision, return 0 on failure."""
    try:
        v = value.strip() if value else "0"
        if v == "":
            v = "0"
        return round(Decimal(v), decimals)
    except (InvalidOperation, ValueError):
        return Decimal("0.00")


def parse_device_string(raw_data: str, debug: bool = False) -> Optional[dict]:
    """
    Parse pipe-delimited device string per gamma.php logic.
    
    Format: IMEI|WAY|data_stamp|CSQ|P1|P2|...|P30|...
    Example: 62360078612541|L|2026-10-10,10:41:22|16|0.00|0.00|...
    
    Returns dict with parsed fields or None if invalid.
    """
    if not raw_data or raw_data.strip() == "":
        if debug:
            logger.debug("PARSE FAIL: raw_data is empty or None")
        return None
    
    raw_data = raw_data.strip()
    fields = raw_data.split("|")
    length = len(fields)
    
    if debug:
        logger.info(f"PARSE DEBUG: raw_data length={len(raw_data)}, field_count={length}")
        logger.info(f"PARSE DEBUG: first 300 chars: {raw_data[:300]}")
        logger.info(f"PARSE DEBUG: fields[0:5] = {fields[:5]}")
    
    # Pad fields to at least 34 (IMEI + WAY + stamp + CSQ + P1-P30)
    while len(fields) < 34:
        fields.append("")
    
    # Need at least 4 fields for minimum valid data
    if length < 4:
        if debug:
            logger.debug(f"PARSE FAIL: only {length} fields (need >= 4)")
        return None
    
    imei = fields[0].strip()
    if not imei:
        if debug:
            logger.debug("PARSE FAIL: imei field is empty")
        return None
    
    way = fields[1].strip()
    
    # Parse data_stamp: format is "YYYY-MM-DD,HH:MM:SS" -> replace comma with space
    data_stamp_raw = fields[2].strip().replace(",", " ")
    try:
        data_stamp = datetime.strptime(data_stamp_raw, "%Y-%m-%d %H:%M:%S")
        # Validate: reject dates beyond 2030 (gamma.php check)
        if data_stamp.year > 2030:
            data_stamp = datetime.now()
    except ValueError:
        data_stamp = datetime.now()
    
    # CSQ: take first part before comma
    csq_raw = fields[3].strip()
    csq = csq_raw.split(",")[0] if csq_raw else "0"
    
    # Parse P1-P30
    params = {}
    for i in range(1, 31):
        params[f"p{i}"] = safe_float(fields[3 + i] if (3 + i) < len(fields) else "0")
    
    return {
        "imei": imei,
        "way": way,
        "data_stamp": data_stamp,
        "csq": csq,
        **params,
    }


# ─── SQL Server Fetch (Primary Source) ───────────────────────────────────────

def fetch_from_sqlserver(imei_list: list, debug: bool = False) -> dict:
    """
    Fetch latest live data for all active IMEIs directly from SQL Server.
    Reads from TB_LIVEDATA (latest state per IMEI, updated by gamma.php).
    
    Returns dict: {imei: parsed_dict, ...}
    """
    results = {}
    try:
        conn = get_mssql_connection()
        cursor = conn.cursor(as_dict=True)
        
        # Fetch all live data rows for our active IMEIs in one query
        # TB_LIVEDATA has: IMEI, WAY, DATA_STAMP, P1-P30, UPDATE_STATUS, SERVER_DT, etc.
        placeholders = ",".join(["%s"] * len(imei_list))
        sql = f"""
            SELECT IMEI, WAY, DATA_STAMP, 
                   P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,
                   P11,P12,P13,P14,P15,P16,P17,P18,P19,P20,
                   P21,P22,P23,P24,P25,P26,P27,P28,P29,P30,
                   UPDATE_STATUS, SERVER_DT, OTHER_3 AS CSQ
            FROM TB_LIVEDATA
            WHERE IMEI IN ({placeholders})
        """
        cursor.execute(sql, tuple(imei_list))
        
        for row in cursor:
            imei = str(row["IMEI"]).strip()
            if not imei:
                continue
            
            # Parse data_stamp
            data_stamp = row.get("DATA_STAMP")
            if isinstance(data_stamp, str):
                data_stamp = data_stamp.replace(",", " ")
                try:
                    data_stamp = datetime.strptime(data_stamp, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    data_stamp = datetime.now()
            elif data_stamp is None:
                data_stamp = datetime.now()
            
            parsed = {
                "imei": imei,
                "way": str(row.get("WAY", "L")).strip(),
                "data_stamp": data_stamp,
                "csq": str(row.get("CSQ", "0")).strip(),
            }
            
            # Extract P1-P30
            for i in range(1, 31):
                val = row.get(f"P{i}", 0)
                try:
                    parsed[f"p{i}"] = round(Decimal(str(val or 0)), 2)
                except (InvalidOperation, ValueError):
                    parsed[f"p{i}"] = Decimal("0.00")
            
            results[imei] = parsed
            
            if debug and len(results) <= 3:
                logger.info(f"MSSQL [{imei}]: stamp={data_stamp}, "
                           f"P1={parsed['p1']}, P5={parsed['p5']}, CSQ={parsed['csq']}")
        
        cursor.close()
        conn.close()
        
        if debug:
            logger.info(f"SQL Server returned data for {len(results)}/{len(imei_list)} IMEIs")
        
    except Exception as e:
        logger.error(f"SQL Server connection failed: {e}")
        logger.error("Check MSSQL_CONFIG in the script. Ensure SQL Server is running.")
    
    return results


def fetch_log_from_sqlserver(imei_list: list, since: datetime, debug: bool = False) -> list:
    """
    Fetch new log entries from TB_LOGEDATA_NEW since the given timestamp.
    This captures the historical timeline of readings, not just the latest.
    
    Returns list of parsed dicts.
    """
    records = []
    try:
        conn = get_mssql_connection()
        cursor = conn.cursor(as_dict=True)
        
        placeholders = ",".join(["%s"] * len(imei_list))
        sql = f"""
            SELECT IMEI, WAY, DATA_STAMP,
                   P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,
                   P11,P12,P13,P14,P15,P16,P17,P18,P19,P20,
                   P21,P22,P23,P24,P25,P26,P27,P28,P29,P30,
                   SERVER_DT, OTHER_1 AS CSQ
            FROM TB_LOGEDATA_NEW
            WHERE IMEI IN ({placeholders})
              AND SERVER_DT > %s
            ORDER BY SERVER_DT
        """
        cursor.execute(sql, tuple(imei_list) + (since,))
        
        for row in cursor:
            imei = str(row["IMEI"]).strip()
            if not imei:
                continue
            
            data_stamp = row.get("DATA_STAMP")
            if isinstance(data_stamp, str):
                data_stamp = data_stamp.replace(",", " ")
                try:
                    data_stamp = datetime.strptime(data_stamp, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    data_stamp = datetime.now()
            elif data_stamp is None:
                data_stamp = datetime.now()
            
            parsed = {
                "imei": imei,
                "way": str(row.get("WAY", "L")).strip(),
                "data_stamp": data_stamp,
                "csq": str(row.get("CSQ", "0")).strip(),
                "server_dt": row.get("SERVER_DT", datetime.now()),
            }
            for i in range(1, 31):
                val = row.get(f"P{i}", 0)
                try:
                    parsed[f"p{i}"] = round(Decimal(str(val or 0)), 2)
                except (InvalidOperation, ValueError):
                    parsed[f"p{i}"] = Decimal("0.00")
            
            records.append(parsed)
        
        cursor.close()
        conn.close()
        
        if debug:
            logger.info(f"SQL Server log: {len(records)} new entries since {since}")
        
    except Exception as e:
        logger.error(f"SQL Server log fetch failed: {e}")
    
    return records


# ─── HTTP Fetch (Fallback) ───────────────────────────────────────────────────

def _extract_data_from_html(html: str, debug: bool = False) -> Optional[str]:
    """
    Extract the pipe-delimited device string from the data-check HTML page.
    
    The page returns an HTML table with columns: S.N., Server Date, Server Time, Data.
    The "Data" column contains the pipe-delimited string we need.
    If no data exists, the page shows "No Data Here!!".
    """
    # Quick check: no data available
    if "No Data Here" in html:
        return None
    
    # Strategy 1: Look for the pipe-delimited string in <td> tags.
    # The data string starts with a 15-digit IMEI followed by |L| or |H|
    # Pattern: 15-digit number followed by pipe-delimited fields
    pattern = r'(\d{9,15}\|[A-Z]\|\d{4}-\d{2}-\d{2}[,\s]\d{2}:\d{2}:\d{2}\|[^<]+)'
    match = re.search(pattern, html)
    if match:
        data_str = match.group(1).strip()
        # Clean any HTML entities
        data_str = data_str.replace('&nbsp;', ' ').replace('&amp;', '&')
        if debug:
            logger.info(f"EXTRACT: Found data string ({len(data_str)} chars): {data_str[:200]}")
        return data_str
    
    # Strategy 2: Find all <td> content and look for pipe-delimited data
    td_pattern = r'<td[^>]*>(.*?)</td>'
    cells = re.findall(td_pattern, html, re.DOTALL)
    for cell in cells:
        cell_clean = cell.strip()
        if '|' in cell_clean and len(cell_clean) > 30:
            # Check if it looks like device data (has multiple pipe-separated fields)
            fields = cell_clean.split('|')
            if len(fields) >= 10:
                if debug:
                    logger.info(f"EXTRACT (td scan): Found data in cell ({len(fields)} fields)")
                return cell_clean
    
    if debug:
        logger.info("EXTRACT: No pipe-delimited data found in HTML")
    return None


def fetch_device_data(session: requests.Session, imei: str, debug: bool = False) -> Optional[str]:
    """
    Fetch data for a single IMEI from the SCADA data-check endpoint.
    The endpoint returns an HTML page; we extract the pipe-delimited
    device string from the table's Data column.
    Returns the raw pipe-delimited string or None on error/no-data.
    """
    try:
        url = f"{DATA_CHECK_URL}?imei={imei}"
        resp = session.get(url, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        html = resp.text
        
        if debug:
            logger.info(f"FETCH DEBUG [{imei}]: status={resp.status_code}, "
                        f"content_type={resp.headers.get('Content-Type','?')}, "
                        f"length={len(html)}")
            logger.info(f"FETCH DEBUG [{imei}]: html_preview={html[:300]}")
        
        # Extract pipe-delimited data from HTML response
        data_str = _extract_data_from_html(html, debug=debug)
        return data_str
        
    except requests.RequestException as e:
        logger.warning(f"HTTP error for IMEI {imei}: {e}")
        return None


# ─── Databricks Push (Monitoring Stats) ──────────────────────────────────────

def _dbx_sql_literal(value):
    """Format a Python value as a safe SQL literal for Databricks SQL."""
    if value is None:
        return "NULL"
    if isinstance(value, (int, float, Decimal)):
        return str(float(value)) if isinstance(value, Decimal) else str(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _chunked(seq, size):
    """Yield fixed-size chunks from a sequence."""
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def push_to_databricks(run_stats: dict, live_data: list):
    """
    Push ingestion stats and live data to Databricks Delta tables.
    This allows monitoring from Databricks without needing ngrok.
    
    Creates/appends to:
      - {schema}.ingestion_runs (one row per cycle)
      - {schema}.device_livedata (full snapshot, overwritten each cycle)
    """
    if not DBX_PUSH_ENABLED:
        return
    
    token = DBX_CONFIG.get("token") or os.environ.get("DATABRICKS_TOKEN", "")
    if not token:
        logger.warning("Databricks push skipped: no token configured.")
        return
    
    try:
        from databricks import sql as dbx_sql
        
        conn = dbx_sql.connect(
            server_hostname=DBX_CONFIG["host"],
            http_path=DBX_CONFIG["http_path"],
            access_token=token,
        )
        cursor = conn.cursor()
        schema = f"{DBX_CONFIG['catalog']}.{DBX_CONFIG['schema']}"
        
        # Create schema if not exists
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        
        # Create ingestion_runs table if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.ingestion_runs (
                run_start TIMESTAMP, run_end TIMESTAMP,
                total_imeis INT, success_count INT, error_count INT,
                no_data_count INT, duration_sec DOUBLE, status STRING
            )
        """)
        
        # Insert run stats
        cursor.execute(f"""
            INSERT INTO {schema}.ingestion_runs VALUES (
                '{run_stats['run_start']}', '{run_stats['run_end']}',
                {run_stats['total_imeis']}, {run_stats['success_count']},
                {run_stats['error_count']}, {run_stats['no_data_count']},
                {run_stats['duration_sec']}, '{run_stats['status']}'
            )
        """)
        
        # Create device_livedata table if not exists
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.device_livedata (
                imei STRING, way STRING, data_stamp TIMESTAMP, csq STRING,
                p1 DOUBLE, p2 DOUBLE, p3 DOUBLE, p4 DOUBLE, p5 DOUBLE,
                p6 DOUBLE, p7 DOUBLE, p8 DOUBLE, p9 DOUBLE, p10 DOUBLE,
                p11 DOUBLE, p12 DOUBLE, p13 DOUBLE, p14 DOUBLE, p15 DOUBLE,
                p16 DOUBLE, p17 DOUBLE, p18 DOUBLE, p19 DOUBLE, p20 DOUBLE,
                p21 DOUBLE, p22 DOUBLE, p23 DOUBLE, p24 DOUBLE, p25 DOUBLE,
                p26 DOUBLE, p27 DOUBLE, p28 DOUBLE, p29 DOUBLE, p30 DOUBLE,
                synced_at TIMESTAMP
            )
        """)
        
        # Overwrite device_livedata with current snapshot using chunked multi-row inserts
        cursor.execute(f"TRUNCATE TABLE {schema}.device_livedata")
        if live_data:
            synced_at = datetime.now()
            columns = (
                "imei, way, data_stamp, csq, "
                "p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, "
                "p11, p12, p13, p14, p15, p16, p17, p18, p19, p20, "
                "p21, p22, p23, p24, p25, p26, p27, p28, p29, p30, synced_at"
            )
            batch_size = 100
            for batch in _chunked(live_data, batch_size):
                values_sql = []
                for d in batch:
                    row_values = [
                        _dbx_sql_literal(d.get("imei")),
                        _dbx_sql_literal(d.get("way")),
                        _dbx_sql_literal(d.get("data_stamp")),
                        _dbx_sql_literal(d.get("csq")),
                    ]
                    row_values.extend(_dbx_sql_literal(d.get(f"p{i}", 0)) for i in range(1, 31))
                    row_values.append(_dbx_sql_literal(synced_at))
                    values_sql.append(f"({', '.join(row_values)})")
                cursor.execute(
                    f"INSERT INTO {schema}.device_livedata ({columns}) VALUES " + ",\n".join(values_sql)
                )
        
        cursor.close()
        conn.close()
        logger.info(f"Pushed stats to Databricks: {schema}.ingestion_runs + device_livedata")
        
    except ImportError:
        logger.warning("databricks-sql-connector not installed. Skipping Databricks push.")
    except Exception as e:
        logger.warning(f"Databricks push failed (non-critical): {e}")


# ─── Database Insert/Upsert ──────────────────────────────────────────────────

def batch_insert_raw(conn, records: list):
    """Batch insert into tb_row_data."""
    if not records:
        return
    sql = """
        INSERT INTO tb_row_data (imei_number, raw_data, server_dt, server_d, url_name)
        VALUES (%s, %s, %s, %s, 'scada_pg_ingestion.py')
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records, page_size=100)


def batch_upsert_livedata(conn, records: list):
    """Batch upsert into tb_livedata (update if exists, insert if not)."""
    if not records:
        return
    sql = """
        INSERT INTO tb_livedata (
            imei, way, data_stamp,
            p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,
            p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,
            p21,p22,p23,p24,p25,p26,p27,p28,p29,p30,
            update_status, server_dt, server_dt_2, url_name, cmd_value, csq
        ) VALUES (
            %s, %s, %s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            'NEW', %s, %s, 'scada_pg_ingestion.py', 'N/A', %s
        )
        ON CONFLICT (imei) DO UPDATE SET
            way = EXCLUDED.way,
            data_stamp = EXCLUDED.data_stamp,
            p1=EXCLUDED.p1, p2=EXCLUDED.p2, p3=EXCLUDED.p3, p4=EXCLUDED.p4,
            p5=EXCLUDED.p5, p6=EXCLUDED.p6, p7=EXCLUDED.p7, p8=EXCLUDED.p8,
            p9=EXCLUDED.p9, p10=EXCLUDED.p10, p11=EXCLUDED.p11, p12=EXCLUDED.p12,
            p13=EXCLUDED.p13, p14=EXCLUDED.p14, p15=EXCLUDED.p15, p16=EXCLUDED.p16,
            p17=EXCLUDED.p17, p18=EXCLUDED.p18, p19=EXCLUDED.p19, p20=EXCLUDED.p20,
            p21=EXCLUDED.p21, p22=EXCLUDED.p22, p23=EXCLUDED.p23, p24=EXCLUDED.p24,
            p25=EXCLUDED.p25, p26=EXCLUDED.p26, p27=EXCLUDED.p27, p28=EXCLUDED.p28,
            p29=EXCLUDED.p29, p30=EXCLUDED.p30,
            update_status = 'NEW',
            server_dt = EXCLUDED.server_dt,
            url_name = 'scada_pg_ingestion.py',
            csq = EXCLUDED.csq
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, records, page_size=100)


# ─── Main Ingestion Cycle ────────────────────────────────────────────────────

def run_ingestion_cycle():
    """Execute one full ingestion cycle for all active IMEIs.
    Reads from SQL Server TB_LIVEDATA and writes to PostgreSQL."""
    cycle_start = datetime.now()
    logger.info("=" * 60)
    logger.info(f"Starting ingestion cycle at {cycle_start}")
    
    pg_conn = get_pg_connection()
    try:
        # Load active IMEIs from PostgreSQL
        imeis = load_active_imeis(pg_conn)
        if not imeis:
            logger.warning("No active IMEIs found. Run --init-db to seed devices.")
            return
        
        logger.info(f"Syncing {len(imeis)} active IMEI devices from SQL Server...")
        
        # Log run start
        run_id = None
        with pg_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO tb_ingestion_log (run_start, total_imeis, status)
                VALUES (%s, %s, 'RUNNING') RETURNING id
            """, (cycle_start, len(imeis)))
            run_id = cur.fetchone()[0]
        pg_conn.commit()
        
        # Fetch live data from SQL Server (single bulk query)
        results = fetch_from_sqlserver(imeis)
        
        logger.info(f"SQL Server returned data for {len(results)}/{len(imeis)} devices.")
        
        # Prepare batch records for PostgreSQL
        now = datetime.now()
        today = now.date()
        raw_records = []
        live_records = []
        error_count = 0
        
        for imei, parsed in results.items():
            try:
                # Build raw data string for tb_row_data
                p_str = "|".join(str(parsed.get(f"p{i}", 0)) for i in range(1, 31))
                raw_data = f"{parsed['imei']}|{parsed['way']}|{parsed['data_stamp']}|{parsed['csq']}|{p_str}"
                
                raw_records.append((
                    parsed["imei"], raw_data, now, today
                ))
                
                # Live data record
                p_values = tuple(parsed[f"p{i}"] for i in range(1, 31))
                live_records.append((
                    parsed["imei"], parsed["way"], parsed["data_stamp"],
                    *p_values,
                    now, now, parsed["csq"]
                ))
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error processing IMEI {imei}: {e}")
        
        # Batch write to PostgreSQL
        if raw_records:
            batch_insert_raw(pg_conn, raw_records)
        if live_records:
            batch_upsert_livedata(pg_conn, live_records)
        pg_conn.commit()
        
        # Update run log
        cycle_end = datetime.now()
        duration = (cycle_end - cycle_start).total_seconds()
        no_data = len(imeis) - len(results)
        
        with pg_conn.cursor() as cur:
            cur.execute("""
                UPDATE tb_ingestion_log 
                SET run_end = %s, success_count = %s, error_count = %s,
                    no_data_count = %s, duration_sec = %s, status = 'COMPLETED'
                WHERE id = %s
            """, (cycle_end, len(live_records), error_count, no_data, duration, run_id))
        pg_conn.commit()
        
        logger.info(f"Cycle complete: {len(live_records)} success, "
                    f"{error_count} errors, {no_data} no-data | "
                    f"Duration: {duration:.1f}s")
        
        # Push monitoring stats to Databricks
        push_to_databricks(
            run_stats={
                "run_start": cycle_start, "run_end": cycle_end,
                "total_imeis": len(imeis), "success_count": len(live_records),
                "error_count": error_count, "no_data_count": no_data,
                "duration_sec": duration, "status": "COMPLETED",
            },
            live_data=list(results.values()),
        )
        
    except Exception as e:
        logger.error(f"Ingestion cycle failed: {e}")
        pg_conn.rollback()
    finally:
        pg_conn.close()


# ─── Health Check ────────────────────────────────────────────────────────────

def show_health():
    """Display ingestion health statistics."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            # Active devices
            cur.execute("SELECT COUNT(*) FROM tb_active_devices WHERE status = 'Active'")
            active = cur.fetchone()[0]
            
            # Total raw records
            cur.execute("SELECT COUNT(*) FROM tb_row_data")
            raw_count = cur.fetchone()[0]
            
            # Live data count
            cur.execute("SELECT COUNT(*) FROM tb_livedata")
            live_count = cur.fetchone()[0]
            
            # Last 5 runs
            cur.execute("""
                SELECT run_start, run_end, total_imeis, success_count, 
                       error_count, no_data_count, duration_sec, status
                FROM tb_ingestion_log 
                ORDER BY run_start DESC LIMIT 5
            """)
            runs = cur.fetchall()
            
            # Stale devices (no update in last 30 min)
            cur.execute("""
                SELECT COUNT(*) FROM tb_livedata 
                WHERE server_dt < NOW() - INTERVAL '30 minutes'
            """)
            stale = cur.fetchone()[0]
            
        print("\n" + "=" * 60)
        print("  SCADA Flow Ingestion - Health Check")
        print("=" * 60)
        print(f"  Active Devices:    {active}")
        print(f"  Raw Data Records:  {raw_count}")
        print(f"  Live Data Records: {live_count}")
        print(f"  Stale (>30 min):   {stale}")
        print("\n  Last 5 Ingestion Runs:")
        print("  " + "-" * 56)
        for r in runs:
            print(f"    {r[0]} | OK:{r[3]} ERR:{r[4]} NODATA:{r[5]} | "
                  f"{r[6]:.1f}s | {r[7]}")
        print("=" * 60 + "\n")
        
    finally:
        conn.close()


# ─── Scan All IMEIs (Find Active Devices) ───────────────────────────────────

def scan_active_devices():
    """Quickly scan all active IMEIs to find which ones currently have data."""
    print("\n" + "=" * 60)
    print("  Scanning all active IMEIs for live data...")
    print("=" * 60)
    
    pg_conn = get_pg_connection()
    try:
        imeis = load_active_imeis(pg_conn)
    finally:
        pg_conn.close()
    
    if not imeis:
        print("  No active IMEIs found. Run --init-db first.")
        return
    
    print(f"  Total IMEIs to scan: {len(imeis)}")
    print(f"  Querying SQL Server...\n")
    
    # Scan SQL Server directly (single bulk query, much faster)
    results = fetch_from_sqlserver(imeis, debug=True)
    
    has_data = [(imei, parsed) for imei, parsed in results.items()]
    no_data_count = len(imeis) - len(has_data)
    
    print("\n" + "=" * 60)
    print(f"  SCAN RESULTS (SQL Server: {MSSQL_CONFIG['database']})")
    print("=" * 60)
    print(f"  With data:    {len(has_data)}")
    print(f"  No data:      {no_data_count}")
    
    if has_data:
        print(f"\n  Sample IMEIs with data (first 10):")
        for imei, parsed in has_data[:10]:
            print(f"    {imei} | stamp={parsed['data_stamp']} | P1={parsed['p1']} P5={parsed['p5']}")
        print(f"\n  Test with: python scada_pg_ingestion.py --test {has_data[0][0]}")
    else:
        print(f"\n  ⚠️  No IMEIs have data in SQL Server.")
        print(f"  Check SQL Server connection and database name in MSSQL_CONFIG.")
    
    print("=" * 60 + "\n")


# ─── Test Single IMEI (Debug Mode) ───────────────────────────────────────────

def test_single_imei(imei: str):
    """Test fetching and parsing for a single IMEI with full debug output."""
    print("\n" + "=" * 60)
    print(f"  Testing IMEI: {imei}")
    print("=" * 60)
    
    # Test SQL Server connection
    print(f"\n  ── SQL Server: {MSSQL_CONFIG['server']} / {MSSQL_CONFIG['database']} ──")
    try:
        results = fetch_from_sqlserver([imei], debug=True)
        if imei in results:
            parsed = results[imei]
            print(f"\n  ✅ SQL SERVER - DATA FOUND")
            print(f"  IMEI:       {parsed['imei']}")
            print(f"  WAY:        {parsed['way']}")
            print(f"  Data Stamp: {parsed['data_stamp']}")
            print(f"  CSQ:        {parsed['csq']}")
            print(f"  Non-zero params:")
            for i in range(1, 31):
                v = parsed[f'p{i}']
                if v != 0:
                    print(f"    P{i} = {v}")
        else:
            print(f"\n  ⚠️  IMEI not found in SQL Server TB_LIVEDATA")
    except Exception as e:
        print(f"\n  ❌ SQL Server error: {e}")
    
    # Also test HTTP endpoint for comparison
    print(f"\n  ── HTTP Endpoint (fallback) ──")
    try:
        session = _build_http_session()
        url = f"{DATA_CHECK_URL}?imei={imei}"
        resp = session.get(url, timeout=HTTP_TIMEOUT)
        if "No Data Here" in resp.text:
            print(f"  HTTP: No Data Here (endpoint has no live data for this IMEI)")
        else:
            data_str = _extract_data_from_html(resp.text)
            if data_str:
                print(f"  HTTP: Data found ({len(data_str)} chars)")
            else:
                print(f"  HTTP: Response received but no pipe-delimited data found")
    except Exception as e:
        print(f"  HTTP: {e}")
    
    print("\n" + "=" * 60)


# ─── Entry Point ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SCADA Flow PostgreSQL Ingestion")
    parser.add_argument("--daemon", action="store_true",
                        help="Run continuously every 15 minutes")
    parser.add_argument("--init-db", action="store_true",
                        help="Initialize database tables and seed IMEI list")
    parser.add_argument("--health", action="store_true",
                        help="Show ingestion health statistics")
    parser.add_argument("--test", type=str, metavar="IMEI",
                        help="Test single IMEI with full debug output")
    parser.add_argument("--scan", action="store_true",
                        help="Scan all IMEIs to find which ones have live data")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL_SECONDS,
                        help=f"Poll interval in seconds (default: {POLL_INTERVAL_SECONDS})")
    args = parser.parse_args()
    
    if args.init_db:
        logger.info("Initializing database...")
        init_database()
        seed_active_devices()
        logger.info("Done. You can now run the ingestion.")
        return
    
    if args.health:
        show_health()
        return
    
    if args.test:
        test_single_imei(args.test)
        return
    
    if args.scan:
        scan_active_devices()
        return
    
    # Single run or daemon mode
    if args.daemon:
        logger.info(f"Starting daemon mode (interval: {args.interval}s / {args.interval/60:.0f} min)")
        while not _shutdown:
            run_ingestion_cycle()
            if _shutdown:
                break
            logger.info(f"Sleeping {args.interval}s until next cycle...")
            # Sleep in small increments for responsive shutdown
            for _ in range(args.interval):
                if _shutdown:
                    break
                time.sleep(1)
        logger.info("Daemon stopped.")
    else:
        run_ingestion_cycle()


if __name__ == "__main__":
    main()
