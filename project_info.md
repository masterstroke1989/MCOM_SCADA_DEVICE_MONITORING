# SCADA Flow Monitoring — Table Schemas & Data Quality Report

**mcom technology** | © 2026 All rights reserved.

---

## Raw Table Schemas

### workspace.scada_flow_2025.tb_logedata (27,279,940 rows)

| Column | Type | Description |
|---|---|---|
| IMEI | STRING | Device IMEI identifier (has whitespace in 13,161 rows) |
| WAY | STRING | Data transmission type: L=Live, S=Stored, R=Resent |
| DATA_STAMP | TIMESTAMP_NTZ | Reading timestamp (contains bogus values: year 1900, 2070) |
| P1–P30 | STRING | 30 sensor parameter values (numeric but stored as strings) |
| SERVER_DT | TIMESTAMP_NTZ | Server receipt timestamp |

### workspace.scada_flow_2026.tb_device_list (608 rows)

| Column | Type | Description |
|---|---|---|
| IDD | LONG | Device internal ID |
| UID | LONG | User/device unique ID (join key to alerts, totalizer) |
| DEVIEC_NAME | STRING | Device name (typo in source) |
| IMEI_NUMBER | STRING | Device IMEI (join key to sensor_log) |
| STATUS | STRING | Active or Deactive |
| OTHER_2/3/4/5 | STRING | Actually latitude/longitude/location_name/site_code |

### workspace.scada_flow_2026.tb_device_list_parameter (18,240 rows)

| Column | Type | Description |
|---|---|---|
| UID | LONG | User/device ID |
| IMEI | STRING | Device IMEI |
| PINDEX | INT | Parameter index (1–30, maps to P1–P30) |
| PDN | STRING | Parameter display name |
| LOW/MAX | STRING | Thresholds (stored as string) |

### workspace.scada_flow_2026.tb_report_alert (67,124 rows)

| Column | Type | Description |
|---|---|---|
| REPORT_DATE | DATE | Alert date |
| ALERT_NAME | STRING | High (37,863) or Low (29,261) |
| ALERT_RANGE | STRING | Contains HTML &nbsp; entities in all rows |
| UID | STRING | User/device ID (stored as string) |

### workspace.scada_flow_2026.tb_report_totalizer (162,134 rows)

| Column | Type | Description |
|---|---|---|
| UID | LONG | User/device ID |
| START_DAY/END_DAY | STRING | Day readings (numeric strings) |
| PNAME | STRING | Inconsistent casing: Total(m3)/TOTAL(m3)/Totalizer(m3) |

---

## Cleaned Table Schemas (workspace.scada_cleaned)

### device_list
| Column | Type | Source | Transform |
|---|---|---|---|
| device_id | LONG | IDD | Direct |
| uid | LONG | UID | Direct |
| device_name | STRING | DEVIEC_NAME | Renamed (fixed typo) |
| installation_date | DATE | INS_DATE | Parsed from string |
| imei | STRING | IMEI_NUMBER | TRIM() |
| service_expiry_date | DATE | SERVICE_DATE | Parsed from string |
| status | STRING | STATUS | Direct (Active/Deactive) |
| latitude | DOUBLE | OTHER_2 | Renamed + cast |
| longitude | DOUBLE | OTHER_3 | Renamed + cast |
| location_name | STRING | OTHER_4 | Renamed |
| site_code | STRING | OTHER_5 | Renamed |

### sensor_log
| Column | Type | Source | Transform |
|---|---|---|---|
| imei | STRING | IMEI | TRIM() |
| data_way | STRING | WAY | Renamed |
| data_timestamp | TIMESTAMP_NTZ | DATA_STAMP | Filtered to 2020–2026 |
| p1–p30 | DOUBLE | P1–P30 | Cast from STRING |

**Deduplication:** ROW_NUMBER() OVER (PARTITION BY IMEI, DATA_STAMP, WAY ORDER BY SERVER_DT DESC)

### alerts
| Column | Type | Source | Transform |
|---|---|---|---|
| alert_type | STRING | ALERT_NAME | Renamed |
| alert_value | DOUBLE | ALERT_VALUE | Cast |
| range_min/range_max | DOUBLE | ALERT_RANGE | REGEXP_EXTRACT after HTML strip |
| uid | INT | UID | Cast |
| parameter_name | STRING | OTHER_2 | Renamed |

### totalizer
| Column | Type | Source | Transform |
|---|---|---|---|
| start_day_reading | DOUBLE | START_DAY | Cast |
| end_day_reading | DOUBLE | END_DAY | Cast |
| daily_consumption | DOUBLE | Computed | END_DAY - START_DAY |
| parameter_name | STRING | PNAME | Normalized casing via CASE |

---

## Data Quality Findings

| # | Issue | Rows | Resolution |
|---|---|---|---|
| 1 | IMEI whitespace | 13,161 | TRIM() |
| 2 | Bogus timestamps (1900) | 3,913 | Filtered out |
| 3 | Bogus timestamps (2070) | 1,209 | Filtered out |
| 4 | Duplicate readings | 25,986 | ROW_NUMBER() dedup |
| 5 | P1-P30 as STRING | All | Cast to DOUBLE |
| 6 | HTML entities in alerts | 67,124 | Strip + regex extract |
| 7 | PNAME casing variants | Multiple | CASE normalization |
| 8 | Empty IDD in totalizer | 20,576 | Documented (not blocking) |
| 9 | Sensor overflow (0xFFFFFFFF) | Sporadic | Documented |
| 10 | Plain-text passwords | 385 | NOT migrated to cleaned schema |

### WAY Value Distribution
| WAY | Count | Percentage |
|---|---|---|
| L (Live) | 27,056,875 | 99.18% |
| S (Stored) | 154,506 | 0.57% |
| R (Resent) | 68,558 | 0.25% |

### Alert Distribution
| Alert Type | Count |
|---|---|
| High | 37,863 |
| Low | 29,261 |

Only 5 devices generate all alerts.

---

## Key Metrics
| Metric | Value |
|---|---|
| Total raw rows | 27,530,444 |
| Raw tables | 9 |
| Cleaned tables | 5 |
| Rows filtered | 5,122 |
| Rows deduped | 25,986 |
| Active devices | 565 |
| Stale active devices (7+ days) | 530+ |
| Peak month | Aug 2025 (4.08M readings) |

---

*© 2026 mcom technology. All rights reserved.*
