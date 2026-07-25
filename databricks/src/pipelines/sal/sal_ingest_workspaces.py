# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC ## DLT SAL — Workspaces
# MAGIC
# MAGIC Auto Loader ingestion of `system.access.workspaces_latest` Parquet (produced by
# MAGIC `raw_extract_workspaces.py`) into `sal_workspaces`.
# MAGIC
# MAGIC **Tier 1** — `system.access.workspaces_latest` is a global account-level table; files land
# MAGIC under `{volume_base_path}/raw/system_access_workspaces_latest/{workspace_name}/` (one
# MAGIC timestamped file per extract run). Auto Loader reads the parent folder and discovers
# MAGIC workspace subfolders recursively.

# COMMAND ----------

from pyspark import pipelines as dp

_bronze = {"quality": "bronze"}

catalog_name = spark.conf.get("pipeline.catalog_name", "it_security")
catalog_env  = spark.conf.get("pipeline.catalog_env",  "dev")
volume_name  = spark.conf.get("pipeline.volume_name",  "system_tables_landing")

catalog          = f"{catalog_name}_{catalog_env}"
bronze_schema        = spark.conf.get("pipeline.schema_bronze", "databricks")
volume_base_path = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}"

# COMMAND ----------

# DBTITLE 1,Workspaces
dp.create_streaming_table("sal_workspaces", table_properties=_bronze,
    comment="SAL staging for system.access.workspaces_latest. One row per workspace per load.")

@dp.append_flow(target="sal_workspaces", name="f_sal_workspaces")
def f_sal_workspaces():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",              "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes",    "true")
        .load(f"{volume_base_path}/raw/system_access_workspaces_latest")
        .selectExpr(
            "*",
            "_metadata.file_name               AS _source_file_name",
            "_metadata.file_modification_time  AS _source_file_dt",
            "current_timestamp()               AS sal_load_dt",
        )
    )
