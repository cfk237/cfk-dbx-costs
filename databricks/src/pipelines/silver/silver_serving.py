# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC ## DLT — Silver AI Gateway Serving
# MAGIC
# MAGIC Delta Live Tables pipeline for `system.serving.*` usage tables:
# MAGIC
# MAGIC | Table | Type | Source |
# MAGIC |---|---|---|
# MAGIC | `tf_serving_usage` | Fact (streaming, append-only) | `{bronze_schema}.serving_endpoint_usage` |
# MAGIC | `td_serving_entity_history` | Dimension (SCD2) | `{bronze_schema}.serving_served_entities` |
# MAGIC | `td_serving_entity` | Dimension (SCD1) | `{bronze_schema}.serving_served_entities` |
# MAGIC
# MAGIC ### Processing strategy
# MAGIC - `tf_serving_usage`: one row per model-serving request, immutable once written — same
# MAGIC   straight streaming-append pattern as `tf_billing_usage` (no `create_auto_cdc_flow()` needed).
# MAGIC - `td_serving_entity_history` / `td_serving_entity`: two `create_auto_cdc_flow()` targets off the
# MAGIC   same source view, keyed on `(account_cd, workspace_cd, served_entity_cd)`, sequenced by
# MAGIC   `change_time` — same SCD2+SCD1 pair pattern as `td_lakeflow_job_history`/
# MAGIC   `td_lakeflow_job`. A served entity's config/version can change over the life of its
# MAGIC   endpoint, and `endpoint_delete_time` arrives as an UPDATE on the same row (surfaced as
# MAGIC   `is_deleted_fl`), not a new row.
# MAGIC - `skipChangeCommits = true` on both streaming reads — consistent with all other bronze
# MAGIC   source reads in this project.
# MAGIC
# MAGIC ### Key design notes
# MAGIC - `external_model_config` / `foundation_model_config` / `custom_model_config` /
# MAGIC   `feature_spec_config` are kept as raw STRING passthroughs (`_lb` suffix) — at most one
# MAGIC   is populated per row depending on `entity_type`; parse in a consumer view if needed.
# MAGIC - `served_entity_id` is a different natural key than `endpoint_id` (already captured as
# MAGIC   `td_billing_endpoint` from billing usage) — one endpoint can serve multiple entities, so
# MAGIC   `td_serving_entity` is kept as its own dimension rather than merged into
# MAGIC   `td_billing_endpoint`; join on `endpoint_cd` downstream if endpoint-level attributes are
# MAGIC   needed alongside a served entity. There is deliberately no separate endpoint-grain
# MAGIC   dimension table — `silver_resource.sql` collapses `td_serving_entity` down to endpoint
# MAGIC   grain inline (`QUALIFY ROW_NUMBER() ... ORDER BY change_time DESC = 1`) when building
# MAGIC   `td_resource`'s ENDPOINT rows, since that was the only consumer that needed it.
# MAGIC
# MAGIC **DLT pipeline configuration (Databricks UI):**
# MAGIC - Target catalog: `{catalog_name}_{catalog_env}`
# MAGIC - Target schema: `global_it_hub`
# MAGIC - Pipeline parameters: `pipeline.catalog_name`, `pipeline.catalog_env`, `pipeline.schema_bronze`
# MAGIC
# MAGIC **First-time setup:**
# MAGIC 1. Bronze tables `{bronze_schema}.serving_endpoint_usage` / `serving_served_entities` must
# MAGIC    already be populated. This file assumes they exist — they are loaded externally
# MAGIC    (outside this repo); request `system.serving.endpoint_usage` / `system.serving.served_entities`
# MAGIC    be added there if they aren't yet available.
# MAGIC 2. Trigger a full-refresh run of the silver pipeline
# MAGIC
# MAGIC Already wired up — no action needed: this file is in the `silver` pipeline's `libraries`
# MAGIC list (`resources/pipelines.yml`), and `td_serving_entity` sentinel rows (-2 Unassigned,
# MAGIC -1 Unknown) are inserted by `init_post_dlt_silver.sql`, matching `td_billing_endpoint`.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, current_timestamp

catalog_name = spark.conf.get("pipeline.catalog_name", "it_security")
catalog_env  = spark.conf.get("pipeline.catalog_env",  "dev")
catalog      = f"{catalog_name}_{catalog_env}"
bronze_schema    = spark.conf.get("pipeline.schema_bronze", "databricks")

_silver = {"quality": "silver"}

# COMMAND ----------

# DBTITLE 1,SERVING USAGE — Source view (column renames + flag cast)
@dp.view(name="v_bz_serving_endpoint_usage")
def v_bz_serving_endpoint_usage():
    return (
        spark.readStream.option("skipChangeCommits", "true")
        .table(f"{catalog}.{bronze_schema}.serving_endpoint_usage")
        .select(
            col("account_id").alias("account_cd"),
            col("workspace_id").alias("workspace_cd"),
            col("region").alias("region_lb"),
            col("databricks_request_id").alias("databricks_request_cd"),
            col("client_request_id").alias("client_request_cd"),
            col("served_entity_id").alias("served_entity_cd"),
            col("requester").alias("requester_lb"),
            col("status_code").alias("status_code_nb"),
            col("request_time").alias("request_dt"),
            col("input_token_count").alias("input_token_count_nb"),
            col("output_token_count").alias("output_token_count_nb"),
            col("input_character_count").alias("input_character_count_nb"),
            col("output_character_count").alias("output_character_count_nb"),
            col("usage_context").alias("usage_context_lb"),
            when(col("request_streaming"), 1).otherwise(0).alias("request_streaming_fl"),
            current_timestamp().alias("sys_load_dt"),
        )
    )

# COMMAND ----------

# DBTITLE 1,tf_serving_usage — Streaming fact table
@dp.table(
    name             = "tf_serving_usage",
    comment          = "Silver fact table of AI Gateway model-serving requests (system.serving.endpoint_usage). One row per request.",
    table_properties = {"quality": "silver"},
)
@dp.expect("databricks_request_cd not null", "databricks_request_cd IS NOT NULL")
@dp.expect("account_cd not null", "account_cd IS NOT NULL")
@dp.expect("request_dt not null", "request_dt IS NOT NULL")
def tf_serving_usage():
    return dp.read_stream("v_bz_serving_endpoint_usage")

# COMMAND ----------

# DBTITLE 1,SERVED ENTITIES — Source view (column renames + flag derivation)
@dp.view(name="v_bz_serving_served_entities")
def v_bz_serving_served_entities():
    return (
        spark.readStream.option("skipChangeCommits", "true")
        .table(f"{catalog}.{bronze_schema}.serving_served_entities")
        .select(
            col("account_id").alias("account_cd"),
            col("workspace_id").alias("workspace_cd"),
            col("served_entity_id").alias("served_entity_cd"),
            col("region").alias("region_lb"),
            col("endpoint_id").alias("endpoint_cd"),
            col("endpoint_name").alias("endpoint_name_lb"),
            col("served_entity_name").alias("served_entity_name_lb"),
            col("entity_type").alias("entity_type_lb"),
            col("entity_name").alias("entity_name_lb"),
            col("entity_version").alias("entity_version_lb"),
            col("endpoint_config_version").alias("endpoint_config_version_nb"),
            col("task").alias("task_lb"),
            col("created_by").alias("created_by_cd"),
            col("external_model_config").alias("external_model_config_lb"),
            col("foundation_model_config").alias("foundation_model_config_lb"),
            col("custom_model_config").alias("custom_model_config_lb"),
            col("feature_spec_config").alias("feature_spec_config_lb"),
            col("change_time"),
            col("endpoint_delete_time").alias("endpoint_delete_dt"),
            when(col("endpoint_delete_time").isNotNull(), 1).otherwise(0).alias("is_deleted_fl"),
            current_timestamp().alias("sys_load_dt"),
        )
    )

# COMMAND ----------

# DBTITLE 1,td_serving_entity_history — SCD2 history (all versions)
dp.create_streaming_table("td_serving_entity_history", table_properties=_silver,
    comment="SCD2 history of AI Gateway served entities — one row per validity period (change_time to next change_time).")

dp.create_auto_cdc_flow(
    target             = "td_serving_entity_history",
    source             = "v_bz_serving_served_entities",
    keys               = ["account_cd", "workspace_cd", "served_entity_cd"],
    sequence_by        = col("change_time"),
    stored_as_scd_type = 2,
)

# COMMAND ----------

# DBTITLE 1,td_serving_entity — SCD1 current (Power BI dimension)
dp.create_streaming_table("td_serving_entity", table_properties=_silver,
    comment="Current state (SCD1) of AI Gateway served entities (system.serving.served_entities) — one row per served entity.",
    schema="""
    entity_id                   BIGINT    GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) COMMENT 'Surrogate key.',
    account_cd                  STRING    NOT NULL COMMENT 'Databricks account UUID.',
    workspace_cd                STRING    NOT NULL COMMENT 'Workspace ID the served entity belongs to.',
    served_entity_cd            STRING    NOT NULL COMMENT 'Natural key — served_entity_id from system.serving.served_entities.',
    region_lb                   STRING    COMMENT 'Databricks region.',
    endpoint_cd                 STRING    COMMENT 'Natural key of the parent serving endpoint — join to td_billing_endpoint.endpoint_cd for endpoint-level attributes.',
    endpoint_name_lb            STRING    COMMENT 'Serving endpoint display name.',
    served_entity_name_lb       STRING    COMMENT 'Served entity display name (user provided).',
    entity_type_lb              STRING    COMMENT 'Entity type — FEATURE_SPEC, EXTERNAL_MODEL, FOUNDATION_MODEL, or CUSTOM_MODEL.',
    entity_name_lb               STRING   COMMENT 'Underlying entity name (e.g. Unity Catalog model name).',
    entity_version_lb           STRING    COMMENT 'Version of the served entity.',
    endpoint_config_version_nb  INT       COMMENT 'Version of the endpoint configuration.',
    task_lb                     STRING    COMMENT 'Task type — llm/v1/chat, llm/v1/completions, or llm/v1/embeddings.',
    created_by_cd                STRING   COMMENT 'User, service principal, or group name that created the served entity.',
    external_model_config_lb    STRING    COMMENT 'External model configuration (raw JSON string, populated when entity_type_lb = EXTERNAL_MODEL).',
    foundation_model_config_lb  STRING    COMMENT 'Foundation model configuration (raw JSON string, populated when entity_type_lb = FOUNDATION_MODEL).',
    custom_model_config_lb      STRING    COMMENT 'Custom model configuration (raw JSON string, populated when entity_type_lb = CUSTOM_MODEL).',
    feature_spec_config_lb      STRING    COMMENT 'Feature spec configuration (raw JSON string, populated when entity_type_lb = FEATURE_SPEC).',
    change_time                 TIMESTAMP COMMENT 'Timestamp of the last change to this served entity — create_auto_cdc_flow sequence key.',
    endpoint_delete_dt           TIMESTAMP COMMENT 'Timestamp the parent endpoint (and this served entity) was deleted.',
    is_deleted_fl                INT      COMMENT 'Flag: 1 if endpoint_delete_dt is set, 0 otherwise.',
    sys_load_dt                  TIMESTAMP COMMENT 'Pipeline audit — timestamp DLT last (re)computed this row. create_auto_cdc_flow() has no insert-only column semantics, so this is re-stamped on every update too, same as sys_load_dt on fact tables — not a true first-insert time.'
""")

dp.create_auto_cdc_flow(
    target             = "td_serving_entity",
    source             = "v_bz_serving_served_entities",
    keys               = ["account_cd", "workspace_cd", "served_entity_cd"],
    sequence_by        = col("change_time"),
    stored_as_scd_type = 1,
)
