# Databricks notebook source
# DBTITLE 1,SAL Ingest - Identities
# MAGIC %md
# MAGIC ## SAL Ingest — Identities (Users & Service Principals)
# MAGIC
# MAGIC Incrementally ingests Parquet files produced by `raw_extract_api_identities.py` into two
# MAGIC SAL Delta tables using **Auto Loader** (`cloudFiles`).
# MAGIC
# MAGIC | Item | Value |
# MAGIC |---|---|
# MAGIC | Source — users | `{volume_base_path}/raw/api_users/` |
# MAGIC | Source — SPs | `{volume_base_path}/raw/api_service_principals/` |
# MAGIC | Target — users | `{catalog}.databricks.sal_users` |
# MAGIC | Target — SPs | `{catalog}.databricks.sal_service_principals` |
# MAGIC | Checkpoints | `{volume_base_path}/checkpoints/api_users/` and `api_service_principals/` |
# MAGIC
# MAGIC Both streams start concurrently and are awaited independently — a failure in one does
# MAGIC not prevent the other from completing.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog_env",  "dev")
dbutils.widgets.text("catalog_name", "it_security")
dbutils.widgets.text("volume_name",  "system_tables_landing")
dbutils.widgets.dropdown("reset_checkpoints", "false", ["false", "true"])

catalog_env       = dbutils.widgets.get("catalog_env")
catalog_name      = dbutils.widgets.get("catalog_name")
volume_name       = dbutils.widgets.get("volume_name")
reset_checkpoints = dbutils.widgets.get("reset_checkpoints") == "true"

schema           = "databricks"
catalog          = f"{catalog_name}_{catalog_env}"
volume_base_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

sources = [
    {
        "source_path"  : f"{volume_base_path}/raw/api_users",
        "checkpoint"   : f"{volume_base_path}/checkpoints/sal/api_users/autoloader/checkpoint",
        "schema_loc"   : f"{volume_base_path}/checkpoints/sal/api_users/autoloader/schema",
        "target_table" : f"{catalog}.{schema}.sal_users",
        "label"        : "api_users",
    },
    {
        "source_path"  : f"{volume_base_path}/raw/api_service_principals",
        "checkpoint"   : f"{volume_base_path}/checkpoints/sal/api_service_principals/autoloader/checkpoint",
        "schema_loc"   : f"{volume_base_path}/checkpoints/sal/api_service_principals/autoloader/schema",
        "target_table" : f"{catalog}.{schema}.sal_service_principals",
        "label"        : "api_service_principals",
    },
]

for s in sources:
    print(f"  {s['label']:30} → {s['target_table']}")
print(f"  reset_checkpoints : {reset_checkpoints}")

# COMMAND ----------

# DBTITLE 1,Reset checkpoints (full re-ingest)
if reset_checkpoints:
    for s in sources:
        checkpoint_dir = f"{volume_base_path}/checkpoints/sal/{s['label']}"
        try:
            dbutils.fs.rm(checkpoint_dir, True)
            print(f"✓ Cleared checkpoint: {checkpoint_dir}")
        except Exception:
            print(f"  (not found: {s['label']})")
        spark.sql(f"TRUNCATE TABLE {s['target_table']}")
        print(f"✓ Truncated: {s['target_table']}")
else:
    print("reset_checkpoints = false — checkpoints kept, incremental load only.")

# COMMAND ----------

# DBTITLE 1,Start Auto Loader streams
queries = []

for s in sources:
    q = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",              "parquet")
        .option("cloudFiles.schemaLocation",      s["schema_loc"])
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes",    "true")
        .load(s["source_path"])
        .selectExpr(
            "*",
            "_metadata.file_name               AS _source_file_name",
            "_metadata.file_modification_time  AS _source_file_dt",
            "current_timestamp()               AS sal_load_dt",
        )
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", s["checkpoint"])
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(s["target_table"])
    )
    queries.append((s["label"], q))
    print(f"✓ Started: {s['label']}  →  {s['target_table']}")

# COMMAND ----------

# DBTITLE 1,Await completion
failures = []
for label, q in queries:
    try:
        q.awaitTermination()
        print(f"✓ Completed: {label}")
    except Exception as e:
        print(f"✗ Failed   : {label} — {e}")
        failures.append((label, e))

if failures:
    summary = ", ".join(l for l, _ in failures)
    raise RuntimeError(f"{len(failures)} stream(s) failed: {summary}")

# COMMAND ----------

# DBTITLE 1,Verify loaded data
for s in sources:
    try:
        count = spark.table(s["target_table"]).count()
        print(f"{s['target_table']}: {count} rows")
    except Exception as e:
        print(f"{s['target_table']}: (not yet created) — {e}")
