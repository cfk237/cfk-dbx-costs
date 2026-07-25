# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC ## DLT — Silver Billing (Usage + Dimensions)
# MAGIC
# MAGIC Delta Live Tables pipeline replacing `silver_billing_usage.sql`.
# MAGIC
# MAGIC | Table | Type | Source |
# MAGIC |---|---|---|
# MAGIC | `tf_billing_usage` | Fact (streaming) | `{bronze_schema}.billing_usage` |
# MAGIC | `td_billing_app` | Dimension SCD1 | `{bronze_schema}.billing_usage` filtered to app_id |
# MAGIC | `td_billing_notebook` | Dimension SCD1 | `{bronze_schema}.billing_usage` filtered to notebook_id |
# MAGIC | `td_billing_endpoint` | Dimension SCD1 | `{bronze_schema}.billing_usage` filtered to endpoint_id |
# MAGIC | `td_billing_network` | Dimension SCD1 | `{bronze_schema}.billing_usage` filtered to networking_client |
# MAGIC | `td_billing_data_quality_monitoring` | Dimension SCD1 | `{bronze_schema}.billing_usage` filtered to billing_origin_product = DATA_QUALITY_MONITORING |
# MAGIC
# MAGIC **`td_billing_list_price` and `tf_billing_cost` are managed by `silver_billing_cost.sql`**
# MAGIC (SQL MERGE notebook run after this DLT pipeline), not by this DLT pipeline. List prices
# MAGIC come from `billing_list_prices` — a full-overwrite snapshot table refreshed externally,
# MAGIC same pattern as `td_workspace`/`access_workspaces_latest` (see `silver_workspace.sql`):
# MAGIC not append-only, so it can't be a DLT streaming source. The MERGE approach for
# MAGIC `tf_billing_cost` also allows price corrections to update `line_cost_usd` on existing
# MAGIC rows — not possible with a DLT streaming append table.
# MAGIC
# MAGIC **DLT pipeline configuration (Databricks UI):**
# MAGIC - Target catalog: `{catalog_name}_{catalog_env}`
# MAGIC - Target schema: `global_it_hub`
# MAGIC - Pipeline parameters: `pipeline.catalog_name`, `pipeline.catalog_env`, `pipeline.schema_bronze`
# MAGIC
# MAGIC **`usage_metadata`** is stored as a raw struct in `tf_billing_usage`.
# MAGIC Resource IDs are flattened only in the consumer view `vf_pbi_billing_cost`.
# MAGIC
# MAGIC **Replaces:** `silver_billing_usage.sql`, `silver_billing_list_prices.sql`
# MAGIC
# MAGIC **First-time setup** and the rationale for declaring each `dp.create_streaming_table()`
# MAGIC immediately before the `create_auto_cdc_flow()` call that populates it (in this same file,
# MAGIC rather than a shared `silver_tables_definition.py`) are documented in `silver_dimensions.py`.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, coalesce, when, current_timestamp, lit

# Pipeline parameters — set in the DLT pipeline configuration UI
catalog_name = spark.conf.get("pipeline.catalog_name", "it_security")
catalog_env  = spark.conf.get("pipeline.catalog_env",  "dev")
catalog      = f"{catalog_name}_{catalog_env}"
bronze_schema    = spark.conf.get("pipeline.schema_bronze", "databricks")

_silver = {"quality": "silver"}

# COMMAND ----------

# DBTITLE 1,Billing usage — Source view
@dp.view(name="v_bz_billing_usage")
def v_bz_billing_usage():
    return (
        spark.readStream.option("skipChangeCommits", "true")
        .table(f"{catalog}.{bronze_schema}.billing_usage")
    )

# COMMAND ----------

# DBTITLE 1,tf_billing_usage — Streaming fact table
@dp.table(
    name             = "tf_billing_usage",
    comment          = "Silver fact table of Databricks billable usage. One row per billing record.",
    table_properties = {"quality": "silver"},
)
@dp.expect("record_id not null", "record_cd IS NOT NULL")
@dp.expect("account_cd not null", "account_cd IS NOT NULL")
@dp.expect("ingestion_dt not null", "ingestion_dt IS NOT NULL")
def tf_billing_usage():
    src = dp.read_stream("v_bz_billing_usage")
    return src.select(
        col("record_id").alias("record_cd"),
        col("account_id").alias("account_cd"),
        col("workspace_id").alias("workspace_cd"),
        col("record_type").alias("record_type_lb"),
        col("ingestion_date").alias("ingestion_dt"),
        col("usage_date").alias("usage_dt"),
        col("usage_start_time").alias("usage_start_dt"),
        col("usage_end_time").alias("usage_end_dt"),
        col("billing_origin_product").alias("product_lb"),
        col("usage_type").alias("usage_type_lb"),
        col("sku_name").alias("sku_name_lb"),
        col("cloud").alias("cloud_lb"),
        col("usage_unit").alias("usage_unit_lb"),
        col("usage_quantity").alias("usage_quantity_nb"),
        col("custom_tags"),
        # Raw struct — resource IDs flattened in consumer view (vf_pbi_billing_cost)
        col("usage_metadata"),
        # Derived identity fields — each identity_metadata field kept as its own column;
        # which one represents "the" identity for a record is a product-specific decision,
        # made downstream in vf_pbi_billing_cost (e.g. DATA_QUALITY_MONITORING uses created_by_cd,
        # everything else falls back through run_as_cd/run_by_cd/created_by_cd/owned_by_cd).
        col("identity_metadata.run_as").alias("run_as_cd"),
        col("identity_metadata.run_by").alias("run_by_cd"),
        col("identity_metadata.created_by").alias("created_by_cd"),
        col("identity_metadata.owned_by").alias("owned_by_cd"),
        col("product_features.jobs_tier").alias("jobs_tier_lb"),
        when(col("product_features.is_serverless"), 1).otherwise(0).alias("is_serverless_fl"),
        when(col("product_features.is_photon"),     1).otherwise(0).alias("is_photon_fl"),
        current_timestamp().alias("sys_load_dt"),
    )

# COMMAND ----------

# DBTITLE 1,td_billing_app — SCD1 dimension (create_auto_cdc_flow)
@dp.view(name="v_bz_billing_app")
def v_bz_billing_app():
    return (
        spark.readStream.option("skipChangeCommits", "true")
        .table(f"{catalog}.{bronze_schema}.billing_usage")
        .filter(col("usage_metadata.app_id").isNotNull())
        .select(
            col("account_id").alias("account_cd"),
            col("workspace_id").alias("workspace_cd"),
            col("usage_metadata.app_id").alias("app_cd"),
            coalesce(
                col("identity_metadata.run_as"),
                col("identity_metadata.run_by"),
                col("identity_metadata.created_by"),
                col("identity_metadata.owned_by"),
            ).alias("run_as_cd"),
            col("billing_origin_product").alias("product_lb"),
            col("usage_metadata.app_name").alias("app_name_lb"),
            col("custom_tags").alias("tags"),
            col("ingestion_date").alias("ingestion_dt"),
            current_timestamp().alias("sys_load_dt"),
        )
    )

dp.create_streaming_table("td_billing_app", table_properties=_silver,
    comment="Current state (SCD1) of Databricks Apps seen in billing usage — one row per app.",
    schema="""
    app_id        BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) COMMENT 'Surrogate key.',
    account_cd    STRING NOT NULL COMMENT 'Databricks account UUID.',
    workspace_cd STRING NOT NULL COMMENT 'Workspace ID the app belongs to.',
    app_cd     STRING NOT NULL COMMENT 'Natural key — app_id from billing usage_metadata.',
    run_as_cd     STRING COMMENT 'User or service principal ID that ran the app (source: identity_metadata).',
    product_lb    STRING COMMENT 'Billing product label associated with the app.',
    app_name_lb   STRING COMMENT 'App display name.',
    tags          MAP<STRING, STRING> COMMENT 'App tags as key/value pairs (source: custom_tags).',
    ingestion_dt   DATE  COMMENT 'Usage record ingestion date this dimension row was first observed.',
    sys_load_dt   TIMESTAMP COMMENT 'Pipeline audit — timestamp DLT last (re)computed this row. create_auto_cdc_flow() has no insert-only column semantics, so this is re-stamped on every update too, same as sys_load_dt on fact tables — not a true first-insert time.'
""")

dp.create_auto_cdc_flow(
    target             = "td_billing_app",
    source             = "v_bz_billing_app",
    keys               = ["account_cd", "workspace_cd", "app_cd"],
    sequence_by        = col("ingestion_dt"),
    stored_as_scd_type = 1,
)

# COMMAND ----------

# DBTITLE 1,td_billing_notebook — SCD1 dimension (create_auto_cdc_flow)
@dp.view(name="v_bz_billing_notebook")
def v_bz_billing_notebook():
    return (
        spark.readStream.option("skipChangeCommits", "true")
        .table(f"{catalog}.{bronze_schema}.billing_usage")
        .filter(col("usage_metadata.notebook_id").isNotNull())
        .select(
            col("account_id").alias("account_cd"),
            col("workspace_id").alias("workspace_cd"),
            col("usage_metadata.notebook_id").alias("notebook_cd"),
            coalesce(
                col("identity_metadata.run_as"),
                col("identity_metadata.run_by"),
                col("identity_metadata.created_by"),
                col("identity_metadata.owned_by"),
            ).alias("run_as_cd"),
            col("billing_origin_product").alias("product_lb"),
            col("usage_metadata.notebook_path").alias("notebook_path_lb"),
            col("custom_tags").alias("tags"),
            col("ingestion_date").alias("ingestion_dt"),
            current_timestamp().alias("sys_load_dt"),
        )
    )

dp.create_streaming_table("td_billing_notebook", table_properties=_silver,
    comment="Current state (SCD1) of notebooks seen in billing usage (e.g. serverless notebook compute) — one row per notebook.",
    schema="""
    notebook_id      BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) COMMENT 'Surrogate key.',
    account_cd       STRING NOT NULL COMMENT 'Databricks account UUID.',
    workspace_cd  STRING NOT NULL COMMENT 'Workspace ID the notebook belongs to.',
    notebook_cd   STRING NOT NULL COMMENT 'Natural key — notebook_id from billing usage_metadata.',
    product_lb       STRING COMMENT 'Billing product label associated with the notebook.',
    notebook_path_lb STRING COMMENT 'Workspace path of the notebook.',
    run_as_cd        STRING COMMENT 'User or service principal ID that ran the notebook (source: identity_metadata).',
    tags             MAP<STRING, STRING> COMMENT 'Notebook tags as key/value pairs (source: custom_tags).',
    ingestion_dt     DATE   COMMENT 'Usage record ingestion date this dimension row was first observed.',
    sys_load_dt      TIMESTAMP COMMENT 'Pipeline audit — timestamp DLT last (re)computed this row. create_auto_cdc_flow() has no insert-only column semantics, so this is re-stamped on every update too, same as sys_load_dt on fact tables — not a true first-insert time.'
""")

dp.create_auto_cdc_flow(
    target             = "td_billing_notebook",
    source             = "v_bz_billing_notebook",
    keys               = ["account_cd", "workspace_cd", "notebook_cd"],
    sequence_by        = col("ingestion_dt"),
    stored_as_scd_type = 1,
)

# COMMAND ----------

# DBTITLE 1,td_billing_endpoint — SCD1 dimension (create_auto_cdc_flow)
@dp.view(name="v_bz_billing_endpoint")
def v_bz_billing_endpoint():
    return (
        spark.readStream.option("skipChangeCommits", "true")
        .table(f"{catalog}.{bronze_schema}.billing_usage")
        .filter(col("usage_metadata.endpoint_id").isNotNull())
        .select(
            col("account_id").alias("account_cd"),
            col("workspace_id").alias("workspace_cd"),
            col("usage_metadata.endpoint_id").alias("endpoint_cd"),
            coalesce(
                col("identity_metadata.run_as"),
                col("identity_metadata.run_by"),
                col("identity_metadata.created_by"),
                col("identity_metadata.owned_by"),
            ).alias("run_as_cd"),
            col("billing_origin_product").alias("product_lb"),
            col("usage_metadata.endpoint_name").alias("endpoint_name_lb"),
            col("custom_tags").alias("tags"),
            col("ingestion_date").alias("ingestion_dt"),
            current_timestamp().alias("sys_load_dt"),
        )
    )

dp.create_streaming_table("td_billing_endpoint", table_properties=_silver,
    comment="Current state (SCD1) of model/feature serving endpoints seen in billing usage — one row per endpoint.",
    schema="""
    endpoint_id      BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) COMMENT 'Surrogate key.',
    account_cd       STRING NOT NULL COMMENT 'Databricks account UUID.',
    workspace_cd  STRING NOT NULL COMMENT 'Workspace ID the endpoint belongs to.',
    endpoint_cd   STRING NOT NULL COMMENT 'Natural key — endpoint_id from billing usage_metadata.',
    product_lb       STRING COMMENT 'Billing product label associated with the endpoint.',
    endpoint_name_lb STRING COMMENT 'Endpoint display name.',
    run_as_cd        STRING COMMENT 'User or service principal ID that ran the endpoint (source: identity_metadata).',
    tags             MAP<STRING, STRING> COMMENT 'Endpoint tags as key/value pairs (source: custom_tags).',
    ingestion_dt     DATE   COMMENT 'Usage record ingestion date this dimension row was first observed.',
    sys_load_dt      TIMESTAMP COMMENT 'Pipeline audit — timestamp DLT last (re)computed this row. create_auto_cdc_flow() has no insert-only column semantics, so this is re-stamped on every update too, same as sys_load_dt on fact tables — not a true first-insert time.'
""")

dp.create_auto_cdc_flow(
    target             = "td_billing_endpoint",
    source             = "v_bz_billing_endpoint",
    keys               = ["account_cd", "workspace_cd", "endpoint_cd"],
    sequence_by        = col("ingestion_dt"),
    stored_as_scd_type = 1,
)

# COMMAND ----------

# DBTITLE 1,td_billing_network — SCD1 dimension (create_auto_cdc_flow)
@dp.view(name="v_bz_billing_network")
def v_bz_billing_network():
    return (
        spark.readStream.option("skipChangeCommits", "true")
        .table(f"{catalog}.{bronze_schema}.billing_usage")
        .filter(
            col("usage_metadata.networking_client").isNotNull()
            | (col("billing_origin_product") == lit("NETWORKING"))
        )
        .select(
            col("account_id").alias("account_cd"),
            col("workspace_id").alias("workspace_cd"),
            coalesce(col("usage_metadata.networking_client"), lit("NETWORKING")).alias("network_client_cd"),
            coalesce(
                col("identity_metadata.run_as"),
                col("identity_metadata.run_by"),
                col("identity_metadata.created_by"),
                col("identity_metadata.owned_by"),
            ).alias("run_as_cd"),
            col("billing_origin_product").alias("product_lb"),
            col("custom_tags").alias("tags"),
            col("ingestion_date").alias("ingestion_dt"),
            current_timestamp().alias("sys_load_dt"),
        )
    )

dp.create_streaming_table("td_billing_network", table_properties=_silver,
    comment="Current state (SCD1) of networking clients seen in billing usage — one row per networking client.",
    schema="""
    network_id        BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) COMMENT 'Surrogate key.',
    account_cd        STRING NOT NULL COMMENT 'Databricks account UUID.',
    workspace_cd   STRING NOT NULL COMMENT 'Workspace ID the networking client belongs to.',
    network_client_cd STRING NOT NULL COMMENT 'Natural key — networking_client from billing usage_metadata.',
    run_as_cd         STRING COMMENT 'User or service principal ID associated with the networking usage (source: identity_metadata).',
    product_lb        STRING COMMENT 'Billing product label associated with the networking client.',
    tags              MAP<STRING, STRING> COMMENT 'Networking tags as key/value pairs (source: custom_tags).',
    ingestion_dt      DATE   COMMENT 'Usage record ingestion date this dimension row was first observed.',
    sys_load_dt       TIMESTAMP COMMENT 'Pipeline audit — timestamp DLT last (re)computed this row. create_auto_cdc_flow() has no insert-only column semantics, so this is re-stamped on every update too, same as sys_load_dt on fact tables — not a true first-insert time.'
""")

dp.create_auto_cdc_flow(
    target             = "td_billing_network",
    source             = "v_bz_billing_network",
    keys               = ["account_cd", "workspace_cd", "network_client_cd"],
    sequence_by        = col("ingestion_dt"),
    stored_as_scd_type = 1,
)

# COMMAND ----------

# DBTITLE 1,td_billing_data_quality_monitoring — SCD1 dimension (create_auto_cdc_flow)
@dp.view(name="v_bz_billing_data_quality_monitoring")
def v_bz_billing_data_quality_monitoring():
    return (
        spark.readStream.option("skipChangeCommits", "true")
        .table(f"{catalog}.{bronze_schema}.billing_usage")
        .filter(col("billing_origin_product") == lit("DATA_QUALITY_MONITORING"))
        .select(
            col("account_id").alias("account_cd"),
            col("workspace_id").alias("workspace_cd"),
            col("usage_metadata.schema_id").alias("dqm_cd"),
            col("identity_metadata.created_by").alias("created_by_cd"),
            col("billing_origin_product").alias("product_lb"),
            col("custom_tags").alias("tags"),
            col("ingestion_date").alias("ingestion_dt"),
            current_timestamp().alias("sys_load_dt"),
        )
    )

dp.create_streaming_table("td_billing_data_quality_monitoring", table_properties=_silver,
    comment="Current state (SCD1) of data quality monitoring schemas seen in billing usage — one row per monitored schema.",
    schema="""
    dqm_id        BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) COMMENT 'Surrogate key.',
    account_cd    STRING NOT NULL COMMENT 'Databricks account UUID.',
    workspace_cd  STRING NOT NULL COMMENT 'Workspace ID the monitored schema belongs to.',
    dqm_cd        STRING NOT NULL COMMENT 'Natural key — schema_id from billing usage_metadata.',
    created_by_cd STRING COMMENT 'User or service principal ID associated with the data quality monitoring usage (source: identity_metadata).',
    product_lb    STRING COMMENT 'Billing product label associated with the monitored schema.',
    tags          MAP<STRING, STRING> COMMENT 'Data quality monitoring tags as key/value pairs (source: custom_tags).',
    ingestion_dt  DATE   COMMENT 'Usage record ingestion date this dimension row was first observed.',
    sys_load_dt   TIMESTAMP COMMENT 'Pipeline audit — timestamp DLT last (re)computed this row. create_auto_cdc_flow() has no insert-only column semantics, so this is re-stamped on every update too, same as sys_load_dt on fact tables — not a true first-insert time.'
""")

dp.create_auto_cdc_flow(
    target             = "td_billing_data_quality_monitoring",
    source             = "v_bz_billing_data_quality_monitoring",
    keys               = ["account_cd", "workspace_cd", "dqm_cd"],
    sequence_by        = col("ingestion_dt"),
    stored_as_scd_type = 1,
)
