# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC ## DLT SAL — System Tables
# MAGIC
# MAGIC Auto Loader ingestion of Parquet files produced by `raw_extract_system_tables.py`
# MAGIC into their `sal_*` staging tables:
# MAGIC
# MAGIC | Source system table | SAL table | Scope |
# MAGIC |---|---|---|
# MAGIC | `system.lakeflow.jobs` | `sal_lakeflow_jobs` | Per workspace |
# MAGIC | `system.lakeflow.pipelines` | `sal_lakeflow_pipelines` | Per workspace |
# MAGIC | `system.compute.clusters` | `sal_compute_clusters` | Per workspace |
# MAGIC | `system.compute.warehouses` | `sal_compute_warehouses` | Per workspace |
# MAGIC | `system.query.history` | `sal_query_history` | Per region (extracted on each workspace) |
# MAGIC | `system.billing.usage` | `sal_billing_usage` | Account-level (central workspace only) |
# MAGIC
# MAGIC **Tier 2** — source files are nested per workspace
# MAGIC (`{volume_base_path}/raw/{raw_table_name}/{workspace_name}/`). A single Auto Loader
# MAGIC flow pointed at the parent `{volume_base_path}/raw/{raw_table_name}` directory
# MAGIC discovers every workspace subfolder recursively. Adding a new workspace requires no
# MAGIC code or config change here.

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

# DBTITLE 1,Auto Loader factory — one flow per logical table, recursive across all workspaces
def _make_system_table(source_table, target_table):
    raw_table_name = source_table.replace(".", "_")
    source_path    = f"{volume_base_path}/raw/{raw_table_name}"

    @dp.append_flow(target=target_table, name=f"f_{target_table}")
    def _flow():
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",              "parquet")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes",    "true")
            .load(source_path)
            .selectExpr(
                "*",
                "_metadata.file_name               AS _source_file_name",
                "_metadata.file_modification_time  AS _source_file_dt",
                "current_timestamp()               AS sal_load_dt",
            )
        )

    return _flow

# COMMAND ----------

# DBTITLE 1,Table declarations (Tier 2)
dp.create_streaming_table("sal_lakeflow_jobs", table_properties=_bronze,
    comment="SAL staging for system.lakeflow.jobs. One row per job version per account.")
dp.create_streaming_table("sal_lakeflow_pipelines", table_properties=_bronze,
    comment="SAL staging for system.lakeflow.pipelines. One row per pipeline version per account.")
dp.create_streaming_table("sal_compute_clusters", table_properties=_bronze,
    comment="SAL staging for system.compute.clusters. One row per cluster version per account.")
dp.create_streaming_table("sal_compute_warehouses", table_properties=_bronze,
    comment="SAL staging for system.compute.warehouses. One row per warehouse version per account.")
dp.create_streaming_table("sal_billing_usage", table_properties=_bronze,
    comment="SAL staging for system.billing.usage. One row per billing record per account.")
dp.create_streaming_table("sal_query_history", table_properties=_bronze,
    comment="SAL staging for system.query.history. One row per statement execution, regional (each workspace sees only its own region).")

# COMMAND ----------

# DBTITLE 1,Register flows for all 6 Tier 2 tables
_make_system_table("system.lakeflow.jobs",      "sal_lakeflow_jobs")
_make_system_table("system.lakeflow.pipelines", "sal_lakeflow_pipelines")
_make_system_table("system.compute.clusters",   "sal_compute_clusters")
_make_system_table("system.compute.warehouses", "sal_compute_warehouses")
_make_system_table("system.billing.usage",      "sal_billing_usage")
_make_system_table("system.query.history",      "sal_query_history")
