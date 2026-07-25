# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC ## DLT — Silver Query History
# MAGIC
# MAGIC Incremental load of `system.query.history` into one silver fact table:
# MAGIC
# MAGIC | Table | Type | Purpose |
# MAGIC |---|---|---|
# MAGIC | `tf_query_history` | Fact (SCD1) | One row per statement execution, account-level |
# MAGIC
# MAGIC ### Processing strategy
# MAGIC - Source: `{bronze_schema}.query_history` (Auto Loader bronze from `system.query.history` Parquet extract)
# MAGIC - `create_auto_cdc_flow()` SCD1, keyed on `(account_cd, workspace_cd, statement_cd)`, sequenced by `update_time`
# MAGIC - `skipChangeCommits = true` on the streaming read — consistent with all other bronze source reads
# MAGIC
# MAGIC ### Key design notes
# MAGIC - `system.query.history` is **regional**: each workspace sees only queries from its own region.
# MAGIC   `{bronze_schema}.query_history` is populated externally (outside this repo) across all
# MAGIC   workspaces before this pipeline runs.
# MAGIC - `compute` and `query_source` are kept as **raw structs** — flatten warehouse_id / resource IDs
# MAGIC   in a consumer view (`vf_pbi_query_history` or equivalent).
# MAGIC - `statement_text` and `error_message` are excluded — both may be encrypted when customer-managed
# MAGIC   keys are configured and have limited analytics value in aggregated cost reporting.
# MAGIC - `update_time` is the sequence key: records are immutable once `execution_status` is final, but
# MAGIC   in-flight queries can receive updates — `create_auto_cdc_flow` SCD1 handles late arrivals cleanly.
# MAGIC
# MAGIC **DLT pipeline configuration (Databricks UI):**
# MAGIC - Target catalog: `{catalog_name}_{catalog_env}`
# MAGIC - Target schema: `global_it_hub`
# MAGIC - Pipeline parameters: `pipeline.catalog_name`, `pipeline.catalog_env`, `pipeline.schema_bronze`
# MAGIC
# MAGIC **First-time setup:**
# MAGIC 1. Ensure `{bronze_schema}.query_history` is populated (loaded externally, outside this repo)
# MAGIC 2. Trigger a full-refresh run of the `silver` DLT pipeline

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, current_timestamp

catalog_name = spark.conf.get("pipeline.catalog_name", "it_security")
catalog_env  = spark.conf.get("pipeline.catalog_env",  "dev")
catalog      = f"{catalog_name}_{catalog_env}"
bronze_schema    = spark.conf.get("pipeline.schema_bronze", "databricks")

_silver = {"quality": "silver"}

# COMMAND ----------

# DBTITLE 1,QUERY HISTORY — Source view (column renames + flag casts)
@dp.view(name="v_bz_query_history")
def v_bz_query_history():
    return (
        spark.readStream.option("skipChangeCommits", "true")
        .table(f"{catalog}.{bronze_schema}.query_history")
        .select(
            col("statement_id").alias("statement_cd"),
            col("account_id").alias("account_cd"),
            col("workspace_id").alias("workspace_cd"),
            col("execution_status").alias("execution_status_lb"),
            col("statement_type").alias("statement_type_lb"),
            col("executed_by").alias("executed_by_lb"),
            col("executed_by_user_id").alias("executed_by_cd"),
            col("executed_as").alias("executed_as_lb"),
            col("executed_as_user_id").alias("executed_as_cd"),
            # Raw structs — flatten compute.warehouse_id / query_source.job_id etc. in consumer view
            col("compute"),
            col("query_source"),
            col("query_tags"),
            col("client_application").alias("client_application_lb"),
            col("client_driver").alias("client_driver_lb"),
            col("start_time").alias("start_dt"),
            col("end_time").alias("end_dt"),
            col("total_duration_ms").alias("total_duration_ms_nb"),
            col("execution_duration_ms").alias("execution_duration_ms_nb"),
            col("compilation_duration_ms").alias("compilation_duration_ms_nb"),
            col("waiting_for_compute_duration_ms").alias("waiting_for_compute_ms_nb"),
            col("waiting_at_capacity_duration_ms").alias("waiting_at_capacity_ms_nb"),
            col("read_rows").alias("read_rows_nb"),
            col("produced_rows").alias("produced_rows_nb"),
            col("read_bytes").alias("read_bytes_nb"),
            col("written_rows").alias("written_rows_nb"),
            col("written_bytes").alias("written_bytes_nb"),
            when(col("from_result_cache"), 1).otherwise(0).alias("from_result_cache_fl"),
            col("update_time"),
            current_timestamp().alias("sys_load_dt"),
        )
    )

# COMMAND ----------

# DBTITLE 1,QUERY HISTORY — Silver fact table (create_auto_cdc_flow SCD1)
# No surrogate key — tf_query_history is a fact table joined by natural keys.
# Schema inferred from source view; no explicit declaration needed.
dp.create_streaming_table(
    "tf_query_history",
    table_properties=_silver,
    comment=(
        "Silver fact table of Databricks query history (system.query.history). "
        "One row per statement execution, account-level. "
        "compute and query_source are raw structs — flatten in consumer views."
    ),
)

dp.create_auto_cdc_flow(
    target             = "tf_query_history",
    source             = "v_bz_query_history",
    keys               = ["account_cd", "workspace_cd", "statement_cd"],
    sequence_by        = col("update_time"),
    stored_as_scd_type = 1,
)
