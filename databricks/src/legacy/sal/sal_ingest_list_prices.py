# Databricks notebook source
# DBTITLE 1,SAL Ingest - List Prices
# MAGIC %md
# MAGIC ## SAL Ingest — List Prices
# MAGIC
# MAGIC Incrementally ingests Parquet files produced by `raw_extract_list_prices.py` into the
# MAGIC `sal_billing_list_prices` SAL Delta table using **Auto Loader** (`cloudFiles`).
# MAGIC
# MAGIC | Item | Value |
# MAGIC |---|---|
# MAGIC | Source | `{volume_base_path}/raw/system_billing_list_prices/` |
# MAGIC | Target | `{catalog}.databricks.sal_billing_list_prices` |
# MAGIC | Checkpoint | `{volume_base_path}/checkpoints/billing_list_prices/autoloader/` |
# MAGIC
# MAGIC **Note:** `system.billing.list_prices` is a global (non-workspace-partitioned) table.
# MAGIC Files are written by a single central `raw_extract_list_prices.py` run into a flat
# MAGIC directory with no workspace partitioning.

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

SOURCE_TABLE    = "system.billing.list_prices"
raw_table_name  = SOURCE_TABLE.replace(".", "_")
sal_table_name  = SOURCE_TABLE.removeprefix("system.").replace(".", "_")
source_path     = f"{volume_base_path}/raw/{raw_table_name}"
checkpoint_path = f"{volume_base_path}/checkpoints/sal/{sal_table_name}/autoloader/checkpoint"
schema_location = f"{volume_base_path}/checkpoints/sal/{sal_table_name}/autoloader/schema"
target_table    = f"{catalog}.{schema}.sal_{sal_table_name}"

print(f"Source path       : {source_path}")
print(f"Target table      : {target_table}")
print(f"Checkpoint        : {checkpoint_path}")
print(f"Reset checkpoints : {reset_checkpoints}")

# COMMAND ----------

# DBTITLE 1,Reset checkpoints (full re-ingest)
# Clears the Auto Loader checkpoint and truncates the SAL table so the next run
# re-ingests all Parquet files from scratch without duplicates.
if reset_checkpoints:
    checkpoint_dir = f"{volume_base_path}/checkpoints/sal/{sal_table_name}"
    try:
        dbutils.fs.rm(checkpoint_dir, True)
        print(f"✓ Cleared checkpoint: {checkpoint_dir}")
    except Exception:
        print(f"  (not found — nothing to clear)")
    spark.sql(f"TRUNCATE TABLE {target_table}")
    print(f"✓ Truncated: {target_table}")
else:
    print("reset_checkpoints = false — checkpoints kept, incremental load only.")

# COMMAND ----------

# DBTITLE 1,Auto Loader - Incremental ingestion with schema evolution
q = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",              "parquet")
    .option("cloudFiles.schemaLocation",      schema_location)
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
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)

q.awaitTermination()
print(f"✓ Auto Loader completed. Data written to: {target_table}")

# COMMAND ----------

# DBTITLE 1,Verify loaded data
df = spark.table(target_table)
print(f"Total rows : {df.count()}")
print(f"Columns    : {len(df.columns)}")
