# SCADA Flow - PostgreSQL Ingestion Setup Guide

## Overview

This project polls SCADA device data from `http://103.96.43.158/scada-flow/data-check`
for 326 active IMEI devices every 15 minutes and loads it into PostgreSQL.

## Architecture

```
[SCADA Devices] --> [HTTP Endpoint: 103.96.43.158]
                         |
                   [scada_pg_ingestion.py]  (runs locally every 15 min)
                         |
                   [PostgreSQL: supply_chain DB]
                    - tb_active_devices  (IMEI list)
                    - tb_row_data        (raw data log)
                    - tb_livedata        (latest per IMEI)
                    - tb_ingestion_log   (run history)
                         |
                   [Databricks Monitoring Notebook]  (via ngrok)
```

## Prerequisites

1. **Python 3.8+** installed locally
2. **PostgreSQL** running on localhost:5432 with `supply_chain` database
3. Install Python dependencies:
   ```
   pip install psycopg2-binary requests
   ```

## Quick Start

### Step 1: Create the PostgreSQL Database

```sql
CREATE DATABASE supply_chain;
```

### Step 2: Initialize Tables and Seed IMEI List

```bash
python scada_pg_ingestion.py --init-db
```

This creates all 4 tables and seeds 326 active IMEI numbers into `tb_active_devices`.

### Step 3: Test Single Run

```bash
python scada_pg_ingestion.py
```

Check the output for success/error counts.

### Step 4: Run as Daemon (Every 15 Minutes)

```bash
python scada_pg_ingestion.py --daemon
```

Press `Ctrl+C` for graceful shutdown.

### Step 5: Check Health

```bash
python scada_pg_ingestion.py --health
```

## Running as Windows Service (Recommended for Production)

### Option A: Windows Task Scheduler

1. Open Task Scheduler (`taskschd.msc`)
2. Create New Task:
   - **Trigger:** Every 15 minutes, repeat indefinitely
   - **Action:** Start a program
     - Program: `python`
     - Arguments: `C:\path\to\scada_pg_ingestion.py`
     - Start in: `C:\path\to\`
3. Enable "Run whether user is logged on or not"

### Option B: Daemon Mode with NSSM

1. Download NSSM from https://nssm.cc/
2. Install as service:
   ```
   nssm install ScadaFlowIngestion python C:\path\to\scada_pg_ingestion.py --daemon
   nssm set ScadaFlowIngestion AppDirectory C:\path\to\
   nssm start ScadaFlowIngestion
   ```

### Option C: Linux Cron

```bash
# Run every 15 minutes
*/15 * * * * cd /path/to && python3 scada_pg_ingestion.py >> /var/log/scada_ingestion.log 2>&1
```

## PostgreSQL Tables

| Table | Purpose | Growth |
|-------|---------|--------|
| `tb_active_devices` | List of IMEIs to poll | Static (326 rows) |
| `tb_row_data` | Raw data strings (append-only) | ~326 rows per 15-min cycle |
| `tb_livedata` | Latest reading per IMEI (upserted) | Max 326 rows |
| `tb_ingestion_log` | Ingestion run history | 1 row per cycle |

## Configuration

Edit the top of `scada_pg_ingestion.py` to change:

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_CONFIG` | localhost:5432/supply_chain | PostgreSQL connection |
| `DATA_CHECK_URL` | http://103.96.43.158/scada-flow/data-check | SCADA endpoint |
| `POLL_INTERVAL_SECONDS` | 900 (15 min) | Daemon poll interval |
| `MAX_WORKERS` | 20 | Concurrent HTTP requests |
| `HTTP_TIMEOUT` | 30s | Per-request timeout |

## Managing IMEI Devices

### Add a new IMEI:
```sql
INSERT INTO tb_active_devices (imei, device_name, status) 
VALUES ('861198063945075', 'New Device', 'Active');
```

### Deactivate an IMEI:
```sql
UPDATE tb_active_devices SET status = 'Deactive' WHERE imei = '861198063945075';
```

## Monitoring from Databricks

1. Start ngrok for PostgreSQL: `ngrok tcp 5432`
2. Update the ngrok address in `04_PG_Ingestion_Monitor` notebook
3. Run the notebook cells to view ingestion stats

## Logs

Logs are written to `logs/scada_ingestion.log` in the script directory.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection refused | Ensure PostgreSQL is running and `supply_chain` DB exists |
| No active IMEIs | Run `python scada_pg_ingestion.py --init-db` to seed devices |
| HTTP timeout | Check if `103.96.43.158` is reachable from your network |
| All no-data | Verify the endpoint URL format: `/data-check?imei=XXXXX` |
