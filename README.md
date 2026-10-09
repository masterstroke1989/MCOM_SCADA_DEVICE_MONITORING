# SCADA Flow Monitoring Project — mcom technology

> Real-time SCADA flow data analytics and device health monitoring platform

## Overview

End-to-end SCADA flow data pipeline: **SQL Server → Databricks Delta Lake → AI/BI Dashboard**.

Ingests data from 2 SQL Server databases (`SCADA_FLOW_2025`, `SCADA_FLOW_2026`) into Unity Catalog Delta tables, performs data cleaning and transformation, and visualizes insights via a 3-page interactive dashboard.

**Total data volume:** 27.5M+ rows across 9 source tables → 5 cleaned analytical tables.

---

## Architecture

```
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────────────────┐
│   SQL Server         │     │   Databricks          │     │   AI/BI Dashboard        │
│   (On-Premises)      │────▶│   Delta Lake           │────▶│   3 Pages, 20+ Widgets   │
│                      │     │                        │     │                          │
│ SCADA_FLOW_2025      │     │ workspace.scada_flow_* │     │ Overview                 │
│ SCADA_FLOW_2026      │     │ workspace.scada_cleaned│     │ Alerts & Consumption     │
└─────────────────────┘     └──────────────────────┘     │ Device Health            │
                                                          └─────────────────────────┘
```

- **Source:** SQL Server (`LAPTOP-954ODF5J\SQLEXPRESS`), databases `SCADA_FLOW_2025` and `SCADA_FLOW_2026`
- **Ingestion:** Local Python script using `pymssql` → pandas → Parquet → UC Volume → `read_files()` → Delta tables
- **Raw tables:** `workspace.scada_flow_2025` and `workspace.scada_flow_2026` schemas (9 tables, 27.5M rows total)
- **Cleaned tables:** `workspace.scada_cleaned` schema (5 tables: `device_list`, `device_parameters`, `sensor_log`, `alerts`, `totalizer`)
- **Dashboard:** mcom technology — SCADA Flow Monitoring Dashboard
- **Warehouse:** Serverless Starter Warehouse

---

## Project Structure

```
MCOM_SCADA_DEVICE_MONITORING/
├── README.md                          # Project overview
├── project_info.md                    # Detailed table schemas & data quality findings
├── 01_Data_Ingestion/                 # Notebook documenting the ingestion pipeline
├── 02_Data_Cleaning_And_Analysis/     # SQL notebook for cleaning & EDA
└── scada_flow_ingestion_local.py      # Local Python ingestion script (run on-prem)
```

---

## Data Pipeline

### Stage 1: Ingestion
1. `pymssql` connects to SQL Server and reads tables in **500K-row chunks**
2. Each chunk is written as a **local Parquet file**
3. Parquet files are **uploaded to UC Staging Volumes** using 4 parallel threads via `databricks-sdk`
4. `read_files()` SQL function creates **Delta tables** from the staged Parquet files

### Stage 2: Cleaning & Transformation
All cleaning outputs land in `workspace.scada_cleaned`:
- IMEI whitespace trimming (13,161 rows)
- P1-P30 STRING → DOUBLE casting
- Bogus timestamp filtering (2020-2026 only, removed 5,122 rows)
- Deduplication via ROW_NUMBER (removed 25,986 duplicates)
- HTML entity stripping in alerts
- PNAME normalization in totalizer

### Stage 3: Visualization
3-page AI/BI Dashboard with 20+ widgets, 20 datasets.

---

## Key Tables

| Table | Rows | Description |
|---|---|---|
| `sensor_log` | ~27.2M | Device sensor readings (P1-P30 as DOUBLE), deduped |
| `device_list` | 608 | Device registry with lat/lon, location, status |
| `device_parameters` | 18,240 | Parameter definitions per device |
| `alerts` | 67,124 | High/Low threshold alerts with min/max ranges |
| `totalizer` | 162,134 | Daily consumption readings |

---

## Key Findings
- **Peak data month:** August 2025 (4.08M readings, 184 active devices)
- **Data way distribution:** 99.3% Live, 0.66% Stored, 0.06% Resent
- **Alert concentration:** Only 5 devices generate all alerts
- **Stale devices:** 530+ active devices have no data for 7+ days

---

## Tech Stack
| Component | Technology |
|---|---|
| Source Database | SQL Server Express (on-premises) |
| Ingestion | Python (pymssql, pandas, pyarrow, databricks-sdk) |
| Storage | Databricks Unity Catalog, Delta Lake |
| Compute | Serverless SQL Warehouse |
| Visualization | AI/BI Dashboard (Lakeview) |

---

## Getting Started

### Prerequisites
```bash
pip install pymssql databricks-sql-connector databricks-sdk pandas pyarrow
```

### Running the Ingestion
1. Ensure SQL Server is running and accessible on port 1433
2. Update `scada_flow_ingestion_local.py` with your Databricks host and token
3. Run: `python scada_flow_ingestion_local.py`

### Running the Cleaning
Open and run all cells in the `02_Data_Cleaning_And_Analysis` notebook in Databricks.

---

## Owner

**mcom technology**

© 2026 mcom technology. All rights reserved.
