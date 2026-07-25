# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC ## DLT SAL — Identities (Users & Service Principals)
# MAGIC
# MAGIC Auto Loader ingestion of SCIM API users/service-principals Parquet (produced by
# MAGIC `raw_extract_api_identities.py`) into `sal_users` / `sal_service_principals`.
# MAGIC
# MAGIC **Tier 1** — files land under `{volume_base_path}/raw/api_users/{workspace_name}/` and
# MAGIC `{volume_base_path}/raw/api_service_principals/{workspace_name}/` (one timestamped file
# MAGIC per extract run). One Auto Loader flow per table reads the parent folder and discovers
# MAGIC workspace subfolders recursively — no per-workspace loop needed.
# MAGIC
# MAGIC Each table's `dp.create_streaming_table()` declaration sits immediately before its
# MAGIC `@dp.append_flow` in this same file (same fix applied to the silver pipeline when
# MAGIC `NO_QUERY_DEFINED_FOR_DATASET` hit a centralized declarations-only notebook).

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

# DBTITLE 1,Users
dp.create_streaming_table("sal_users", table_properties=_bronze,
    comment="SAL staging for Databricks SCIM API users. One row per user per load run.")

@dp.append_flow(target="sal_users", name="f_sal_users")
def f_sal_users():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",              "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes",    "true")
        .load(f"{volume_base_path}/raw/api_users")
        .selectExpr(
            "*",
            "_metadata.file_name               AS _source_file_name",
            "_metadata.file_modification_time  AS _source_file_dt",
            "current_timestamp()               AS sal_load_dt",
        )
    )

# COMMAND ----------

# DBTITLE 1,Service principals
dp.create_streaming_table("sal_service_principals", table_properties=_bronze,
    comment="SAL staging for Databricks SCIM API service principals. One row per SP per load run.")

@dp.append_flow(target="sal_service_principals", name="f_sal_service_principals")
def f_sal_service_principals():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",              "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes",    "true")
        .load(f"{volume_base_path}/raw/api_service_principals")
        .selectExpr(
            "*",
            "_metadata.file_name               AS _source_file_name",
            "_metadata.file_modification_time  AS _source_file_dt",
            "current_timestamp()               AS sal_load_dt",
        )
    )
