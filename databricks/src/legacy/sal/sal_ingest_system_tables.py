# Databricks notebook source
# DBTITLE 1,SAL Ingest - System Tables
# MAGIC %md
# MAGIC ## SAL Ingest — System Tables
# MAGIC
# MAGIC Incrementally ingests Parquet files produced by `raw_extract_system_tables.py` into
# MAGIC source-aligned SAL Delta tables using **Auto Loader** (`cloudFiles`).
# MAGIC
# MAGIC | Item | Value |
# MAGIC |---|---|
# MAGIC | Source pattern | `{volume_base_path}/raw/{raw_table_name}/` (recursive — all workspace subfolders) |
# MAGIC | Target pattern | `{catalog}.databricks.sal_{sal_table_name}` (e.g. `sal_lakeflow_jobs`, `system_` prefix stripped) |
# MAGIC | Checkpoints | `{volume_base_path}/checkpoints/sal/{sal_table_name}/` |
# MAGIC
# MAGIC **Tables:** `system.lakeflow.jobs`, `system.lakeflow.pipelines`, `system.compute.clusters`,
# MAGIC `system.compute.warehouses`, `system.query.history`, `system.billing.usage`
# MAGIC
# MAGIC **Features:**
# MAGIC - One Auto Loader stream per table — discovers all workspace subfolders recursively; no workspace list needed
# MAGIC - Each table lands in its own SAL Delta table; all workspace sources are appended into the same target
# MAGIC - Lineage columns: `_source_file_name`, `_source_file_dt`, `_ingestion_dt`

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog_env",           "dev")
dbutils.widgets.text("catalog_name",          "it_security")
dbutils.widgets.text("volume_name",           "system_tables_landing")
dbutils.widgets.dropdown("reset_checkpoints", "false", ["false", "true"])

catalog_env       = dbutils.widgets.get("catalog_env")
catalog_name      = dbutils.widgets.get("catalog_name")
volume_name       = dbutils.widgets.get("volume_name")
reset_checkpoints = dbutils.widgets.get("reset_checkpoints") == "true"

schema           = "databricks"
catalog          = f"{catalog_name}_{catalog_env}"
volume_base_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

print(f"Catalog          : {catalog}")
print(f"Volume base path : {volume_base_path}")

# COMMAND ----------

# DBTITLE 1,Table list
SAL_TABLES = [
    "system.lakeflow.jobs",
    "system.lakeflow.pipelines",
    "system.compute.clusters",
    "system.compute.warehouses",
    "system.query.history",
    "system.billing.usage",
]

print("Tables to load:")
for t in SAL_TABLES:
    print(f"  {t}  →  {catalog}.{schema}.sal_{t.removeprefix('system.').replace('.', '_')}")

# COMMAND ----------

# DBTITLE 1,Reset checkpoints (full re-ingest)
# Clears Auto Loader checkpoints and truncates SAL Delta tables so the next run
# re-ingests all Parquet files from scratch without duplicates.
# Must be used together with reset_checkpoints = true in raw_extract_system_tables.py (raw layer) —
# resetting only one layer causes duplicates in the SAL tables.
if reset_checkpoints:
    for source_table in SAL_TABLES:
        sal_table_name  = source_table.removeprefix("system.").replace(".", "_")
        checkpoint_dir  = f"{volume_base_path}/checkpoints/sal/{sal_table_name}"
        target_table    = f"{catalog}.{schema}.sal_{sal_table_name}"

        try:
            dbutils.fs.rm(checkpoint_dir, True)
            print(f"✓ Cleared checkpoint: {checkpoint_dir}")
        except Exception:
            print(f"  (not found: {checkpoint_dir})")

        spark.sql(f"TRUNCATE TABLE {target_table}")
        print(f"✓ Truncated: {target_table}")
else:
    print("reset_checkpoints = false — checkpoints kept, incremental load only.")

# COMMAND ----------

# DBTITLE 1,Start Auto Loader streams
# One stream per table — Auto Loader discovers all workspace subfolders recursively.
queries = []

for source_table in SAL_TABLES:
    raw_table_name  = source_table.replace(".", "_")
    sal_table_name  = source_table.removeprefix("system.").replace(".", "_")
    checkpoint_base = f"{volume_base_path}/checkpoints/sal/{sal_table_name}"
    source_path     = f"{volume_base_path}/raw/{raw_table_name}"
    target_table    = f"{catalog}.{schema}.sal_{sal_table_name}"

    q = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",              "parquet")
        .option("cloudFiles.schemaLocation",      f"{checkpoint_base}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes",    "true")
        .load(source_path)
        .selectExpr(
            "*",
            "_metadata.file_name               AS _source_file_name",
            "_metadata.file_modification_time  AS _source_file_dt",
            "current_timestamp()               AS sal_load_dt",
        )
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/checkpoint")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    queries.append((source_table, q))
    print(f"✓ Started: {source_table}  →  {target_table}")

print(f"\n{len(queries)} stream(s) started — awaiting completion...")

# COMMAND ----------

# DBTITLE 1,Await completion
# Each stream is awaited independently so a failure in one table does not
# prevent the others from finishing. Failures are raised together at the end.
failures = []
for source_table, q in queries:
    try:
        q.awaitTermination()
        print(f"✓ Completed: {source_table}")
    except Exception as e:
        print(f"✗ Failed   : {source_table} — {e}")
        failures.append((source_table, e))

if failures:
    summary = ", ".join(t for t, _ in failures)
    raise RuntimeError(f"{len(failures)} stream(s) failed: {summary}")

# COMMAND ----------

# DBTITLE 1,Verify loaded data
for source_table in SAL_TABLES:
    sal_table_name = source_table.removeprefix("system.").replace(".", "_")
    target_table   = f"{catalog}.{schema}.sal_{sal_table_name}"
    try:
        count = spark.table(target_table).count()
        print(f"{target_table}: {count} rows")
    except Exception as e:
        print(f"{target_table}: (not yet created) — {e}")
