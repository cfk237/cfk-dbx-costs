# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC ## Raw Extract — Streaming (System Tables to Parquet)
# MAGIC
# MAGIC Extracts system table data incrementally via Spark Structured Streaming and writes
# MAGIC timestamped Parquet files to a Unity Catalog Volume.
# MAGIC
# MAGIC | Item | Value |
# MAGIC |---|---|
# MAGIC | Default tables | `system.lakeflow.jobs`, `system.lakeflow.pipelines`, `system.compute.clusters`, `system.compute.warehouses` |
# MAGIC | Optional | `system.billing.usage` — enable via `include_billing_usage = true` |
# MAGIC | Default tables | `system.query.history` — regional table, extracted on every workspace alongside the others |
# MAGIC | Output | `{volume_base_path}/raw/{table_safe_name}/{workspace_name}/{yyyy-mm-dd-hhmmss}-{workspace_name}-{table_safe_name}.parquet` |
# MAGIC | State tracking | Checkpoint directories (not `_pipeline_watermarks`) |
# MAGIC
# MAGIC **Deployment model:**
# MAGIC - Run with `include_billing_usage = false` on **each regional workspace** to collect dimension data
# MAGIC - Run with `include_billing_usage = true` on **one central workspace** to also collect billing usage
# MAGIC
# MAGIC **Streaming constraints (Databricks docs):**
# MAGIC - `skipChangeCommits = true` is mandatory — system tables use Delta Sharing and upstream deletes break streams
# MAGIC - `Trigger.AvailableNow` is NOT supported with Delta Sharing — silently converted to `Trigger.Once`
# MAGIC - 7-day checkpoint retention — stream must not lag >7 days or checkpoint becomes stale

# COMMAND ----------

# DBTITLE 1,Setup — widgets
from datetime import datetime
import os
import shutil
import glob

dbutils.widgets.text("catalog_env",      "dev")
dbutils.widgets.text("catalog_name",     "it_security")
dbutils.widgets.text("schema",           "databricks")
dbutils.widgets.text("volume_name",      "system_tables_landing")
dbutils.widgets.dropdown("include_billing_usage", "true", ["false", "true"])
dbutils.widgets.dropdown("reset_checkpoints",     "false", ["false", "true"])

catalog_env           = dbutils.widgets.get("catalog_env")
catalog_name          = dbutils.widgets.get("catalog_name")
schema                = dbutils.widgets.get("schema")
volume_name           = dbutils.widgets.get("volume_name")
include_billing_usage = dbutils.widgets.get("include_billing_usage") == "true"
reset_checkpoints     = dbutils.widgets.get("reset_checkpoints") == "true"

catalog          = f"{catalog_name}_{catalog_env}"
volume_base_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

# apiUrl is populated in every execution context (interactive, jobs, serverless),
# unlike browserHostName which only exists in interactive sessions.
ctx            = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url  = ctx.apiUrl().get().replace("https://", "")
workspace_name = workspace_url.split(".")[0]

print(f"Catalog              : {catalog}")
print(f"Schema               : {schema}")
print(f"Volume base path     : {volume_base_path}")
print(f"Workspace            : {workspace_name}")
print(f"Include billing.usage: {include_billing_usage}")
print(f"Reset checkpoints    : {reset_checkpoints}")

# COMMAND ----------

# DBTITLE 1,Table list
STREAMING_TABLES = [
    "system.lakeflow.jobs",
    "system.lakeflow.pipelines",
    "system.compute.clusters",
    "system.compute.warehouses",
    "system.query.history",
]

if include_billing_usage:
    STREAMING_TABLES.append("system.billing.usage")

print("Tables to extract:")
for t in STREAMING_TABLES:
    print(f"  {t}")

# COMMAND ----------

# DBTITLE 1,Reset checkpoints (full re-extract)
# Clears checkpoint directories and archives (moves, not deletes) Parquet output for every
# table in STREAMING_TABLES. The checkpoint must still be cleared so the stream restarts from
# scratch; archiving — instead of deleting — the Parquet output keeps it out of the active
# `raw/` path (so the downstream SAL Auto Loader does not re-ingest it as duplicates) while
# preserving it under `archive/raw/` for audit/debugging.
if reset_checkpoints:
    reset_timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    for table in STREAMING_TABLES:
        table_safe_name = table.replace(".", "_")
        checkpoint_dir  = f"{volume_base_path}/checkpoints/raw/{table_safe_name}"
        output_dir      = f"{volume_base_path}/raw/{table_safe_name}/{workspace_name}"
        archive_dir     = f"{volume_base_path}/archive/raw/{table_safe_name}/{workspace_name}/{reset_timestamp}"

        try:
            dbutils.fs.mv(output_dir, archive_dir, True)
            print(f"✓ Archived: {output_dir} → {archive_dir}")
        except Exception:
            print(f"  (not found — nothing to archive: {output_dir})")

        try:
            dbutils.fs.rm(checkpoint_dir, True)
            print(f"✓ Cleared checkpoint: {checkpoint_dir}")
        except Exception:
            print(f"  (not found — nothing to clear: {checkpoint_dir})")
else:
    print("reset_checkpoints = false — checkpoints kept, incremental extract only.")

# COMMAND ----------

# DBTITLE 1,Stream helpers
def make_batch_writer(table_safe_name, output_dir, workspace_name):
    """Return a foreachBatch handler that writes each micro-batch as a single named Parquet file."""
    def write_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            print(f"[{table_safe_name}] Batch {batch_id}: empty, skipping.")
            return

        timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        file_name = f"{timestamp}-{workspace_name}-{table_safe_name}.parquet"
        temp_path = f"{output_dir}/_tmp/batch_{batch_id}"
        final_path = f"{output_dir}/{file_name}"

        # Write as single file via coalesce, then rename the part file
        batch_df.coalesce(1).write.mode("overwrite").parquet(temp_path)

        part_files = glob.glob(f"{temp_path}/*.parquet")
        if part_files:
            os.rename(part_files[0], final_path)

        shutil.rmtree(temp_path, ignore_errors=True)
        print(f"[{table_safe_name}] Batch {batch_id}: wrote {batch_df.count()} rows → {final_path}")

    return write_batch


def start_stream(source_table):
    """Start a streaming query for one system table. Returns the StreamingQuery object."""
    table_safe_name     = source_table.replace(".", "_")
    checkpoint_location = f"{volume_base_path}/checkpoints/raw/{table_safe_name}/checkpoint"
    schema_location     = f"{volume_base_path}/checkpoints/{table_safe_name}/schema"
    output_dir          = f"{volume_base_path}/raw/{table_safe_name}/{workspace_name}"

    dbutils.fs.mkdirs(output_dir)

    stream_df = (
        spark.readStream
        .option("skipChangeCommits", "true")
        .option("schemaTrackingLocation", schema_location)
        .option("mergeSchema", "true")
        .table(source_table)
    )

    query = (
        stream_df.writeStream
        .foreachBatch(make_batch_writer(table_safe_name, output_dir, workspace_name))
        .option("checkpointLocation", checkpoint_location)
        .trigger(availableNow=True)
        .start()
    )

    print(f"✓ Started stream: {source_table} → {output_dir}")
    return query

# COMMAND ----------

# DBTITLE 1,Start streams
queries = []
for table in STREAMING_TABLES:
    q = start_stream(table)
    queries.append((table, q))

print(f"\n{len(queries)} stream(s) started — awaiting completion...")

# COMMAND ----------

# DBTITLE 1,Await completion
# Each stream is awaited independently so a failure in one table does not prevent the others
# from finishing. Failures are collected and raised together at the end so the notebook/job
# exits with an error, triggering any configured Databricks retry policy.
failures = []
for table, q in queries:
    try:
        q.awaitTermination()
        print(f"✓ Completed: {table}")
    except Exception as e:
        print(f"✗ Failed   : {table} — {e}")
        failures.append((table, e))

if failures:
    failed_tables = ", ".join(t for t, _ in failures)
    raise RuntimeError(f"{len(failures)} stream(s) failed: {failed_tables}")

# COMMAND ----------

# DBTITLE 1,Verify output
for table in STREAMING_TABLES:
    table_safe_name = table.replace(".", "_")
    output_dir = f"{volume_base_path}/raw/{table_safe_name}/{workspace_name}"
    try:
        files = dbutils.fs.ls(output_dir)
        parquet_files = [f for f in files if f.name.endswith(".parquet") and not f.name.startswith("_")]
        print(f"{table_safe_name}: {len(parquet_files)} parquet file(s)")
        for f in sorted(parquet_files, key=lambda x: x.name, reverse=True)[:3]:
            print(f"  {f.name}  ({f.size / 1024:.1f} KB)")
    except Exception as e:
        print(f"{table_safe_name}: (directory empty or not yet created) — {e}")
