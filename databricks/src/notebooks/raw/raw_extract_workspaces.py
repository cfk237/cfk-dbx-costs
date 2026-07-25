# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC ## Raw Extract — Workspace
# MAGIC
# MAGIC Extracts `system.access.workspaces_latest` as a full-refresh Parquet file in a Unity Catalog Volume.
# MAGIC
# MAGIC | Item | Value |
# MAGIC |---|---|
# MAGIC | Source | `system.access.workspaces_latest` |
# MAGIC | Output | `{volume_base_path}/raw/system_access_workspaces_latest/{workspace_name}/{yyyy-mm-dd-hhmmss}-{workspace_name}-system_access_workspaces_latest.parquet` (new file per run) |
# MAGIC | Strategy | Full extract — new timestamped file per run, no watermark, no streaming |
# MAGIC | Retention | Extracts older than `retention_days` (default 7) are deleted at the end of each run |
# MAGIC
# MAGIC **Note:** `system.access.workspaces_latest` does not support Structured Streaming.
# MAGIC A full snapshot on each run is appropriate given the table's small size
# MAGIC (one row per workspace in the account, typically < 20 rows).
# MAGIC
# MAGIC **Why a new file per run (not overwrite):** the SAL Auto Loader tracks ingested files by
# MAGIC *path* in its checkpoint — an overwritten file with the same name is considered already
# MAGIC processed and silently skipped after the first load. File names must be unique per run.

# COMMAND ----------

# DBTITLE 1,Setup — widgets
from datetime import datetime, timezone

dbutils.widgets.text("catalog_env",      "dev")
dbutils.widgets.text("catalog_name",     "it_security")
dbutils.widgets.text("schema",           "databricks")
dbutils.widgets.text("folder_base_path", "system_tables_landing")
dbutils.widgets.text("retention_days",   "7")

catalog_env      = dbutils.widgets.get("catalog_env")
catalog_name     = dbutils.widgets.get("catalog_name")
schema           = dbutils.widgets.get("schema")
folder_base_path = dbutils.widgets.get("folder_base_path")
retention_days   = int(dbutils.widgets.get("retention_days"))

catalog          = f"{catalog_name}_{catalog_env}"
volume_base_path = f"/Volumes/{catalog}/{schema}/{folder_base_path}"

# apiUrl is populated in every execution context (interactive, jobs, serverless),
# unlike browserHostName which only exists in interactive sessions.
ctx            = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url  = ctx.apiUrl().get().replace("https://", "")
workspace_name = workspace_url.split(".")[0]

SOURCE_TABLE    = "system.access.workspaces_latest"
TABLE_SAFE_NAME = SOURCE_TABLE.replace(".", "_")
# Workspace subfolder — same layout as raw_extract_system_tables.py; the SAL Auto Loader
# reads the parent table folder and discovers workspace subfolders recursively.
output_dir      = f"{volume_base_path}/raw/{TABLE_SAFE_NAME}/{workspace_name}"

# Unique name per run — Auto Loader never reprocesses an already-seen path, so the
# extract must land as a new file (same pattern as raw_extract_system_tables.py).
run_ts          = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
final_path      = f"{output_dir}/{run_ts}-{workspace_name}-{TABLE_SAFE_NAME}.parquet"

print(f"Catalog   : {catalog}")
print(f"Workspace : {workspace_name}")
print(f"Output    : {final_path}")

# COMMAND ----------

# DBTITLE 1,Extract — system.access.workspaces_latest
row_count = spark.table(SOURCE_TABLE).count()
print(f"Rows to extract: {row_count}")

if row_count > 0:
    temp_path = f"{output_dir}/_tmp"
    dbutils.fs.mkdirs(output_dir)

    spark.table(SOURCE_TABLE).coalesce(1).write.mode("overwrite").parquet(temp_path)

    part_file = next(f.path for f in dbutils.fs.ls(temp_path) if f.name.endswith(".parquet"))
    dbutils.fs.mv(part_file, final_path)
    dbutils.fs.rm(temp_path, True)

    print(f"✓ Written {row_count} rows → {final_path}")
else:
    print("⚠ Source table is empty — file not written")

# COMMAND ----------

# DBTITLE 1,Verify output
row_count = spark.read.parquet(final_path).count()
print(f"Row count : {row_count}")

# COMMAND ----------

# DBTITLE 1,Prune expired extracts
# Storage hygiene only — Auto Loader has already checkpointed processed files, so deleting
# old extracts never causes re-ingestion. Keeps directory listing fast as runs accumulate.
import time

cutoff_ms = int(time.time() * 1000) - retention_days * 86400 * 1000
for f in dbutils.fs.ls(output_dir):
    if f.name.endswith(".parquet") and f.modificationTime < cutoff_ms:
        dbutils.fs.rm(f.path)
        print(f"Deleted expired extract: {f.name}")
