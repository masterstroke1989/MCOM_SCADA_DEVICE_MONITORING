# Databricks notebook source
# DBTITLE 1,SCADA Flow - PostgreSQL Ingestion Monitor
# MAGIC %md
# MAGIC # SCADA Flow - PostgreSQL Ingestion Monitor
# MAGIC
# MAGIC This notebook monitors the SCADA data ingestion pipeline that loads device data into PostgreSQL.
# MAGIC
# MAGIC **Architecture:**
# MAGIC - Local Python script (`scada_pg_ingestion.py`) reads from **SQL Server** (`SCADA_FLOW_2026.TB_LIVEDATA`) every 15 minutes
# MAGIC - 319/326 active IMEIs synced to **PostgreSQL** (`supply_chain` DB) in ~0.3s per cycle
# MAGIC - Target tables: `tb_row_data` (raw append log), `tb_livedata` (latest per IMEI), `tb_ingestion_log` (run history)
# MAGIC - Scheduled via Windows Task Scheduler for auto-restart on reboot
# MAGIC
# MAGIC **Monitoring:** The ingestion script pushes stats to Databricks Delta tables after each cycle.
# MAGIC No ngrok needed — data flows: SQL Server → PostgreSQL (local) + Databricks Delta (cloud).

# COMMAND ----------

# DBTITLE 1,Configuration
# Monitoring schema — the ingestion script pushes data here
MONITOR_SCHEMA = "workspace.scada_flow_monitoring"

# Create schema if first time
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MONITOR_SCHEMA}")
print(f"Monitoring schema: {MONITOR_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Ingestion Run History
# Ingestion run history — pushed by the local script after each cycle
df_runs = spark.table(f"{MONITOR_SCHEMA}.ingestion_runs")
print(f"Total runs logged: {df_runs.count()}")
df_runs.orderBy("run_start", ascending=False).limit(20).display()

# COMMAND ----------

# DBTITLE 1,Current Device Status (Live Data Snapshot)
# Live data snapshot — overwritten by the script each cycle
df_live = spark.table(f"{MONITOR_SCHEMA}.device_livedata")
print(f"Total devices with live data: {df_live.count()}")
df_live.orderBy("synced_at", ascending=False).limit(20).display()

# COMMAND ----------

# DBTITLE 1,Stale Devices - No Update in 30+ Minutes
from pyspark.sql.functions import col, current_timestamp, expr

df_stale = df_live.filter(
    col("synced_at") < current_timestamp() - expr("INTERVAL 30 MINUTES")
).select("imei", "data_stamp", "synced_at", "csq")

print(f"Stale devices (>30 min since last sync): {df_stale.count()}")
df_stale.orderBy("synced_at").display()

# COMMAND ----------

# DBTITLE 1,Device Data Distribution
from pyspark.sql.functions import count, countDistinct, to_date

# Device readings distribution by data_stamp date
df_live.withColumn("data_date", to_date("data_stamp")).groupBy("data_date").agg(
    count("*").alias("devices"),
).orderBy("data_date", ascending=False).limit(14).display()

# COMMAND ----------

# DBTITLE 1,Cross-Reference: Databricks vs Synced Device Coverage
# Compare active devices in Databricks vs synced live data
df_dbx = spark.sql("""
    SELECT imei, device_name, site_code, service_expiry_date
    FROM workspace.scada_cleaned.device_list
    WHERE status = 'Active' AND service_expiry_date >= current_date()
""")

synced_count = df_live.count()
print(f"Databricks active IMEIs:  {df_dbx.count()}")
print(f"Synced devices with data: {synced_count}")

# IMEIs active in Databricks but not in live sync
df_missing = df_dbx.join(df_live, df_dbx.imei == df_live.imei, "left_anti")
missing_count = df_missing.count()
if missing_count > 0:
    print(f"\nActive IMEIs without live data: {missing_count}")
    df_missing.select("imei", "device_name", "site_code").display()
else:
    print("\nAll active devices have live data synced.")

# COMMAND ----------

# DBTITLE 1,Ingestion Success Rate Trend
from pyspark.sql.functions import col, round as spark_round, to_date

# Success rate over time
df_trend = df_runs.filter(col("status") == "COMPLETED").withColumn(
    "run_date", to_date("run_start")
).withColumn(
    "success_rate", spark_round(col("success_count") / col("total_imeis") * 100, 1)
).select("run_start", "run_date", "total_imeis", "success_count", 
         "error_count", "no_data_count", "duration_sec", "success_rate")

print(f"Total completed runs: {df_trend.count()}")
df_trend.orderBy("run_start", ascending=False).limit(50).display()

# COMMAND ----------

# DBTITLE 1,Pipeline Health Summary
from pyspark.sql.functions import avg, max as spark_max, min as spark_min, sum as spark_sum

# Overall pipeline health
if df_trend.count() > 0:
    df_summary = df_trend.agg(
        spark_sum("success_count").alias("total_records_synced"),
        avg("success_rate").alias("avg_success_rate"),
        avg("duration_sec").alias("avg_duration_sec"),
        spark_max("run_start").alias("last_run"),
        spark_min("run_start").alias("first_run"),
    )
    df_summary.display()
    
    row = df_summary.collect()[0]
    print(f"\n--- Quick Stats ---")
    print(f"  Total records synced:  {row['total_records_synced']}")
    print(f"  Avg success rate:      {row['avg_success_rate']:.1f}%")
    print(f"  Avg cycle duration:    {row['avg_duration_sec']:.2f}s")
    print(f"  First run:             {row['first_run']}")
    print(f"  Last run:              {row['last_run']}")
else:
    print("No completed runs yet. Run the ingestion script with Databricks push enabled.")
    print("Set DBX_PUSH_ENABLED = True and configure your token in scada_pg_ingestion.py")

# COMMAND ----------

# DBTITLE 1,Historical Log Data (device_logedata)
# Historical log data — appended every cycle (NOT overwritten like device_livedata)
df_log = spark.table(f"{MONITOR_SCHEMA}.device_logedata")
print(f"Total historical log records: {df_log.count()}")
print(f"Unique devices in log: {df_log.select('imei').distinct().count()}")
df_log.orderBy("server_dt", ascending=False).select(
    "imei", "way", "data_stamp", "p1", "p2", "p3", "csq", "server_dt", "server_d"
).limit(20).display()

# COMMAND ----------

# DBTITLE 1,Log Records Per Cycle (Growth Over Time)
from pyspark.sql.functions import count, countDistinct, to_timestamp, date_format

# Records per ingestion cycle — shows how the log grows over time
df_log_cycles = df_log.groupBy("server_dt").agg(
    count("*").alias("records"),
    countDistinct("imei").alias("unique_devices")
).orderBy("server_dt", ascending=False)

print(f"Total ingestion cycles in log: {df_log_cycles.count()}")
df_log_cycles.limit(20).display()

# COMMAND ----------

# DBTITLE 1,Daily Log Volume Trend
from pyspark.sql.functions import sum as spark_sum

# Daily volume — how many log records per day
df_daily = df_log.groupBy("server_d").agg(
    count("*").alias("total_records"),
    countDistinct("imei").alias("unique_devices"),
    countDistinct("server_dt").alias("cycles")
).orderBy("server_d", ascending=False)

print("Daily log volume:")
df_daily.limit(14).display()

# COMMAND ----------

# DBTITLE 1,Top Active Devices (Most Log Entries)
# Which devices have the most historical readings?
df_top_devices = df_log.groupBy("imei").agg(
    count("*").alias("total_readings"),
    spark_max("data_stamp").alias("latest_stamp"),
    spark_min("data_stamp").alias("earliest_stamp"),
).orderBy("total_readings", ascending=False)

print(f"Devices with log data: {df_top_devices.count()}")
df_top_devices.limit(20).display()