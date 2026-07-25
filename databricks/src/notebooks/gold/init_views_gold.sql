-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Gold — Power BI Views (`dataplatform_usage`)
-- MAGIC
-- MAGIC Creates gold views for Power BI semantic models.
-- MAGIC Safe to re-run: every statement uses `CREATE OR REPLACE VIEW`.
-- MAGIC
-- MAGIC | View | Source | Layer |
-- MAGIC |---|---|---|
-- MAGIC | `vd_pbi_resource` | `global_it_hub.td_resource` | Dimension |
-- MAGIC | `vd_pbi_identity` | `global_it_hub.td_identity` | Dimension |
-- MAGIC | `vd_pbi_workspace` | `global_it_hub.td_workspace` | Dimension |
-- MAGIC | `vd_pbi_tag_key` | `global_it_hub.td_tag_key` | Dimension |
-- MAGIC | `vd_pbi_serving_entity` | `global_it_hub.td_serving_entity` | Dimension |
-- MAGIC | `vd_pbi_dbx_group` | `global_it_hub.td_dbx_group` | Dimension |
-- MAGIC | `vr_pbi_resource_tag` | `global_it_hub.tr_resource_tag` | Bridge |
-- MAGIC | `vr_pbi_dbx_group_member` | `global_it_hub.tr_dbx_group_member` | Bridge |
-- MAGIC | `vr_pbi_dbx_user_security` | `global_it_hub.tr_dbx_user_security` | Bridge |
-- MAGIC | `vf_pbi_billing_cost` | `global_it_hub.tf_billing_cost` × dims | Fact |
-- MAGIC | `vf_pbi_query_cost` | `global_it_hub.mvf_query_cost_attribution` × dims | Fact |
-- MAGIC | `vf_pbi_serving_cost` | `global_it_hub.mvf_serving_cost_attribution` × dims | Fact (v0) |
-- MAGIC
-- MAGIC ### Design principles
-- MAGIC - Views (not tables): sources are flat, pre-keyed silver dimensions — no joins or aggregation needed
-- MAGIC - Pipeline audit columns (`sys_create_dt`, `sys_update_dt`) retained — useful for data freshness checks in PBI
-- MAGIC - Schema `dataplatform_usage` isolates the consumer surface from `global_it_hub` internals
-- MAGIC - Sentinel rows (negative IDs) are always included in dimension views, never filtered —
-- MAGIC   fact views resolve unmatched FKs to these sentinels (see e.g. `vf_pbi_billing_cost`),
-- MAGIC   so a dimension view that filters them out breaks that relationship in Power BI

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog_env", "dev")
-- MAGIC catalog_env = dbutils.widgets.get("catalog_env")
-- MAGIC
-- MAGIC dbutils.widgets.text("catalog_name", "it_security")
-- MAGIC catalog_name = dbutils.widgets.get("catalog_name")
-- MAGIC
-- MAGIC catalog     = f"{catalog_name}_{catalog_env}"
-- MAGIC gold_schema = "dataplatform_usage"
-- MAGIC
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC spark.sql(f"USE SCHEMA {gold_schema}")
-- MAGIC
-- MAGIC print(f"Catalog : {catalog}")
-- MAGIC print(f"Schema  : {gold_schema}")

-- COMMAND ----------

-- DBTITLE 1,vd_pbi_resource
-- Dimension view: one row per resource (job, warehouse, pipeline, cluster, app, notebook, endpoint).
-- Sentinel rows (resource_id < 0) are included, not excluded — see Design principles above.
-- Thin wrapper over global_it_hub.td_resource — zero storage cost, always current.
CREATE OR REPLACE VIEW dataplatform_usage.vd_pbi_resource AS
SELECT
  resource_id,
  INITCAP(resource_type_lb) AS resource_type_lb,
  source_cd,
  account_cd,
  workspace_cd,
  display_name_lb,
  owned_by_cd,
  CASE WHEN is_active_fl = 1 THEN 'Yes' ELSE 'No' END AS is_active_fl,
  sys_create_dt,
  sys_update_dt
FROM global_it_hub.td_resource;

-- COMMAND ----------

-- DBTITLE 1,vd_pbi_identity
-- Dimension view: one row per user or service principal.
-- Sentinel rows (identity_id < 0) are included, not excluded — see Design principles above.
-- Thin wrapper over global_it_hub.td_identity — zero storage cost, always current.
CREATE OR REPLACE VIEW dataplatform_usage.vd_pbi_identity AS
SELECT
  identity_id,
  account_cd,
  INITCAP(identity_type_lb) AS identity_type_lb,
  source_cd,
  display_name_lb,
  identity_cd,
  first_name_lb,
  last_name_lb,
  CASE WHEN is_active_fl = 1 THEN 'Yes' ELSE 'No' END AS is_active_fl,
  sys_create_dt,
  sys_update_dt
FROM global_it_hub.td_identity;

-- COMMAND ----------

-- DBTITLE 1,vd_pbi_workspace
-- Dimension view: one row per Databricks workspace.
-- Sentinel rows (workspace_id < 0) are included, not excluded — see Design principles above.
-- Thin wrapper over global_it_hub.td_workspace — zero storage cost, always current.
CREATE OR REPLACE VIEW dataplatform_usage.vd_pbi_workspace AS
SELECT
  workspace_id,
  account_cd,
  workspace_cd,
  workspace_lb,
  CASE
    WHEN LOWER(workspace_lb) LIKE '%dev%'     THEN 'Development'
    WHEN LOWER(workspace_lb) LIKE '%staging%' THEN 'Staging'
    WHEN LOWER(workspace_lb) LIKE '%prod%'    THEN 'Production'
    ELSE 'Other'
  END AS environment_lb,
  -- Same branch precedence as environment_lb so each label maps to exactly one order value
  CASE
    WHEN LOWER(workspace_lb) LIKE '%dev%'     THEN 3
    WHEN LOWER(workspace_lb) LIKE '%staging%' THEN 2
    WHEN LOWER(workspace_lb) LIKE '%prod%'    THEN 1
    ELSE 4
  END AS environment_order_nb,
  workspace_url_lb,
  create_dt,
  status_lb,
  CASE WHEN is_deleted_fl = 1 THEN 'Yes' ELSE 'No' END AS is_deleted_fl,
  sys_create_dt,
  sys_update_dt
FROM global_it_hub.td_workspace;

-- COMMAND ----------

-- DBTITLE 1,vd_pbi_tag_key
-- Dimension view: one row per distinct tag key.
-- Sentinel rows (tag_key_id < 0) are included, not excluded — see Design principles above.
-- tag_key_cd is the normalized consumer label (e.g. AppId).
-- raw_key_lb is the original MAP key before normalization (e.g. app_id) — exposed for reference.
CREATE OR REPLACE VIEW dataplatform_usage.vd_pbi_tag_key AS
SELECT
  tag_key_id,
  INITCAP(tag_key_cd) AS tag_key_cd,
  sys_create_dt,
  sys_update_dt
FROM global_it_hub.td_tag_key;

-- COMMAND ----------

-- DBTITLE 1,vd_pbi_serving_entity
-- Dimension view: one row per AI Gateway served entity (system.serving.served_entities).
-- Sentinel rows (entity_id < 0) are included, not excluded — see Design principles above.
-- vf_pbi_serving_cost falls back to entity_id = -1 when it can't resolve a real entity
-- (COALESCE(t5.entity_id, -1)); filtering the sentinel out here left that relationship
-- with no matching dimension row in Power BI.
-- Thin wrapper over global_it_hub.td_serving_entity — zero storage cost, always current.
-- Dropped: external_model_config_lb / foundation_model_config_lb / custom_model_config_lb /
--          feature_spec_config_lb (raw JSON strings, not consumable by Power BI directly —
--          same reasoning as custom_tags/query_tags being dropped elsewhere in this file).
-- Power BI relationship:
--   vd_pbi_serving_entity.entity_id → vf_pbi_serving_cost.entity_id
CREATE OR REPLACE VIEW dataplatform_usage.vd_pbi_serving_entity AS
SELECT
  entity_id,
  account_cd,
  workspace_cd,
  served_entity_cd,
  region_lb,
  endpoint_cd,
  endpoint_name_lb,
  served_entity_name_lb,
  INITCAP(entity_type_lb) AS entity_type_lb,
  entity_name_lb,
  entity_version_lb,
  endpoint_config_version_nb,
  task_lb,
  created_by_cd,
  change_time,
  CASE WHEN is_deleted_fl = 1 THEN 'Yes' ELSE 'No' END AS is_deleted_fl,
  sys_load_dt
FROM global_it_hub.td_serving_entity;

-- COMMAND ----------

-- DBTITLE 1,vd_pbi_dbx_group
-- Dimension view: one row per SCIM group.
-- Sentinel rows (-2 Unassigned, -1 Unknown) are included, not excluded — see Design principles above.
-- Thin wrapper over global_it_hub.td_dbx_group — zero storage cost, always current.
CREATE OR REPLACE VIEW dataplatform_usage.vd_pbi_dbx_group AS
SELECT
  group_id,
  account_cd,
  group_cd,
  display_name_lb,
  CASE WHEN is_active_fl = 1 THEN 'Yes' ELSE 'No' END AS is_active_fl,
  sys_create_dt,
  sys_update_dt
FROM global_it_hub.td_dbx_group;

-- COMMAND ----------

-- DBTITLE 1,vr_pbi_resource_tag
-- Bridge view: one row per (resource_id, tag_key_id).
-- Resolves resource × tag key combinations; tag_value_lb = 'Untagged' when the resource
-- does not carry the key.
-- Rows linked to sentinels are excluded (resource_id > 0 AND tag_key_id > 0).
-- Power BI relationships:
--   vd_pbi_resource.resource_id  → vr_pbi_resource_tag.resource_id
--   vd_pbi_tag_key.tag_key_id    → vr_pbi_resource_tag.tag_key_id
CREATE OR REPLACE VIEW dataplatform_usage.vr_pbi_resource_tag AS
SELECT
  resource_id,
  tag_key_id,
  tag_value_lb,
  sys_create_dt,
  sys_update_dt
FROM global_it_hub.tr_resource_tag
WHERE resource_id > 0
  AND tag_key_id  > 0;

-- COMMAND ----------

-- DBTITLE 1,vr_pbi_dbx_group_member
-- Bridge view: one row per (group, member) SCIM membership edge — see tr_dbx_group_member
-- (silver_identity.sql) for how member_cd/member_type_lb are parsed from the SCIM $ref path.
-- No sentinel-row filter (unlike vr_pbi_resource_tag's integer FKs): group_cd/member_cd here
-- are always genuine SCIM IDs straight from account_groups, never synthesized sentinel codes.
-- is_deleted_fl is surfaced, not filtered — same treatment as is_active_fl elsewhere in this
-- file — so report authors can choose to include or exclude revoked memberships.
-- Power BI relationships:
--   vd_pbi_dbx_group.group_cd → vr_pbi_dbx_group_member.group_cd
CREATE OR REPLACE VIEW dataplatform_usage.vr_pbi_dbx_group_member AS
SELECT
  account_cd,
  group_cd,
  member_cd,
  member_type_lb,
  CASE WHEN is_deleted_fl = 1 THEN 'Yes' ELSE 'No' END AS is_deleted_fl,
  sys_create_dt,
  sys_update_dt
FROM global_it_hub.tr_dbx_group_member;

-- COMMAND ----------

-- DBTITLE 1,vr_pbi_dbx_user_security
-- Bridge view: one row per (user, visible member) pair — see tr_dbx_user_security
-- (silver_user_security.sql) for how the recursive group-nesting flattening is computed.
-- Power BI relationship:
--   vd_pbi_identity.identity_cd → vr_pbi_dbx_user_security.user_cd  (or member_cd, per RLS use case)
CREATE OR REPLACE VIEW dataplatform_usage.vr_pbi_dbx_user_security AS
SELECT
  user_cd,
  member_cd,
  sys_create_dt,
  sys_update_dt
FROM global_it_hub.tr_dbx_user_security;

-- COMMAND ----------

-- DBTITLE 1,vf_pbi_billing_cost
-- Fact view: one row per billing record enriched with resolved integer FKs.
-- The `billing_cost_resolved` CTE computes each ambiguous field once (from usage_metadata /
-- product_lb / the four identity_metadata columns) instead of repeating the same CASE/COALESCE
-- logic inline everywhere it's needed — every downstream reference is a plain column read.
-- resource_id: `t3` joins td_resource on `resolved_resource_type_lb` + `resolved_source_cd`.
--   Falls back to the per-type unknown sentinel (e.g. -3 Z_UNKNOWN_JOB, keyed off
--   resolved_resource_type_lb) when a resource ID is present in billing but td_resource has no
--   matching row. Falls back to -2 (Z_UNDEFINED) when the record has no resource context at all
--   (e.g. storage-level costs) or its type has no dedicated sentinel (e.g. PREDICTIVE_OPTIMIZATION).
-- identity_id: `t4` joins td_identity.identity_cd on `resolved_identity_cd` (username for users,
--   application ID for service principals). DATA_QUALITY_MONITORING uses created_by_cd (schema
--   monitors are created, not "run"); every other product falls back through
--   COALESCE(run_as_cd, run_by_cd, created_by_cd, owned_by_cd). Exposed as `run_as_cd` in the
--   SELECT list for backward compatibility with existing reports. When not found in td_identity,
--   falls back by shape: GUID-formatted → -4 (Z_UNKNOWN_SP, application IDs are GUIDs),
--   containing '@' → -3 (Z_UNKNOWN_USER, usernames are emails), NULL → -2 (Unassigned),
--   anything else → -1 (Unknown).
-- Dropped: custom_tags (MAP), individual resource ID columns,
--          cloud_lb, usage_start/end_dt, sys_load_dt.
-- Power BI relationships:
--   vd_pbi_workspace.workspace_id → vf_pbi_billing_cost.workspace_id
--   vd_pbi_resource.resource_id   → vf_pbi_billing_cost.resource_id
--   vd_pbi_identity.identity_id   → vf_pbi_billing_cost.identity_id
CREATE OR REPLACE VIEW dataplatform_usage.vf_pbi_billing_cost AS
WITH billing_cost_resolved AS (
  SELECT
    *,
    CASE
      WHEN product_lb = 'DATA_QUALITY_MONITORING' THEN created_by_cd
      ELSE COALESCE(run_as_cd, run_by_cd, created_by_cd, owned_by_cd)
    END AS resolved_identity_cd,
    CASE
      WHEN usage_metadata.job_id            IS NOT NULL THEN 'JOB'
      WHEN usage_metadata.dlt_pipeline_id   IS NOT NULL THEN 'PIPELINE'
      WHEN usage_metadata.warehouse_id      IS NOT NULL THEN 'WAREHOUSE'
      WHEN usage_metadata.cluster_id        IS NOT NULL THEN 'CLUSTER'
      WHEN usage_metadata.app_id            IS NOT NULL THEN 'APP'
      WHEN usage_metadata.notebook_id       IS NOT NULL THEN 'NOTEBOOK'
      WHEN usage_metadata.endpoint_id       IS NOT NULL THEN 'ENDPOINT'
      WHEN usage_metadata.networking_client IS NOT NULL OR product_lb = 'NETWORKING' THEN 'NETWORKING'
      WHEN usage_metadata.schema_id         IS NOT NULL THEN 'DATA_QUALITY_MONITORING'
      WHEN product_lb = 'PREDICTIVE_OPTIMIZATION' THEN 'PREDICTIVE_OPTIMIZATION'
      ELSE product_lb
    END AS resolved_resource_type_lb,
    COALESCE(
      usage_metadata.job_id,
      usage_metadata.dlt_pipeline_id,
      usage_metadata.warehouse_id,
      usage_metadata.cluster_id,
      usage_metadata.app_id,
      usage_metadata.notebook_id,
      usage_metadata.endpoint_id,
      usage_metadata.schema_id,
      CASE WHEN usage_metadata.networking_client IS NOT NULL OR product_lb = 'NETWORKING'
           THEN COALESCE(usage_metadata.networking_client, 'NETWORKING')
           WHEN product_lb = 'PREDICTIVE_OPTIMIZATION' THEN 'PREDICTIVE_OPTIMIZATION'
           ELSE product_lb
      END
    ) AS resolved_source_cd
  FROM global_it_hub.tf_billing_cost
)
SELECT
  t1.record_cd,
  t1.ingestion_dt,
  t1.usage_dt,
  t1.account_cd,
  COALESCE(t2.workspace_id, -3)       AS workspace_id,
  t1.record_type_lb,
  t1.product_lb,
  t1.usage_type_lb,
  t1.sku_name_lb,
  t1.usage_unit_lb,
  t1.usage_quantity_nb,
  t1.line_cost_usd,
  CASE WHEN t1.is_serverless_fl = 1 THEN 'Yes' ELSE 'No' END AS is_serverless_fl,
  CASE WHEN t1.is_photon_fl    = 1 THEN 'Yes' ELSE 'No' END AS is_photon_fl,
  t1.jobs_tier_lb,
  -- usage_metadata flattened here so the silver tables (tf_billing_cost, tf_billing_usage)
  -- remain agnostic to new resource types added by Databricks.
  t1.usage_metadata.job_id            AS job_cd,
  t1.usage_metadata.cluster_id        AS cluster_cd,
  t1.usage_metadata.warehouse_id      AS warehouse_cd,
  t1.usage_metadata.dlt_pipeline_id   AS pipeline_cd,
  t1.usage_metadata.app_id            AS app_cd,
  t1.usage_metadata.notebook_id       AS notebook_cd,
  t1.usage_metadata.endpoint_id       AS endpoint_cd,
  t1.usage_metadata.networking_client AS network_client_cd,
  t1.usage_metadata.schema_id         AS dqm_cd,
  t1.usage_metadata.node_type         AS node_type_lb,
  t1.resolved_identity_cd             AS run_as_cd,
  COALESCE(
    t3.resource_id,
    CASE t1.resolved_resource_type_lb
      WHEN 'JOB'                     THEN -3   -- Z_UNKNOWN_JOB
      WHEN 'PIPELINE'                THEN -4   -- Z_UNKNOWN_PIPELINE
      WHEN 'CLUSTER'                 THEN -5   -- Z_UNKNOWN_CLUSTER
      WHEN 'WAREHOUSE'               THEN -6   -- Z_UNKNOWN_WAREHOUSE
      WHEN 'APP'                     THEN -7   -- Z_UNKNOWN_APP
      WHEN 'NOTEBOOK'                THEN -8   -- Z_UNKNOWN_NOTEBOOK
      WHEN 'ENDPOINT'                THEN -9   -- Z_UNKNOWN_ENDPOINT
      WHEN 'NETWORKING'              THEN -10  -- Z_UNKNOWN_NETWORKING
      WHEN 'DATA_QUALITY_MONITORING' THEN -11  -- Z_UNKNOWN_DQM
      ELSE -2                                  -- Z_UNDEFINED (no resource context, or a type with no dedicated sentinel e.g. PREDICTIVE_OPTIMIZATION)
    END
  )                                   AS resource_id,
  COALESCE(
    t4.identity_id,
    CASE
      WHEN NULLIF(t1.resolved_identity_cd, '') IS NULL THEN  0                      -- System-User (no identity)
      WHEN t1.resolved_identity_cd RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        THEN -4                                                                     -- Z_UNKNOWN_SP (GUID-shaped)
      WHEN t1.resolved_identity_cd LIKE '%@%' THEN -3                               -- Z_UNKNOWN_USER (email-shaped)
      ELSE -1                                                                       -- Unknown
    END
  )                                    AS identity_id
FROM billing_cost_resolved AS t1
LEFT JOIN global_it_hub.td_workspace AS t2
  ON  t2.account_cd      = t1.account_cd
  AND t2.workspace_cd = t1.workspace_cd
LEFT JOIN global_it_hub.td_resource AS t3
  ON  t3.account_cd       = t1.account_cd
  AND t3.workspace_cd     = t1.workspace_cd
  AND t3.resource_type_lb = t1.resolved_resource_type_lb
  AND t3.source_cd        = t1.resolved_source_cd
LEFT JOIN global_it_hub.td_identity AS t4
  ON  t4.account_cd  = t1.account_cd
  AND t4.identity_cd = t1.resolved_identity_cd;

-- COMMAND ----------

-- DBTITLE 1,vf_pbi_query_cost
-- Fact view: one row per statement execution with its allocated warehouse cost.
-- Source: mvf_query_cost_attribution (MV owned by the attributed_cost DLT pipeline,
-- pipelines/silver/silver_query_cost_attribution.sql — refreshed after silver_billing_cost).
-- Integer FKs resolved like vf_pbi_billing_cost:
--   workspace_id → td_workspace (-3 = unknown workspace)
--   resource_id  → td_resource WAREHOUSE rows (-6 = Z_UNKNOWN_WAREHOUSE)
--   identity_id  → td_identity via executed_by (fallback by shape: GUID → -4 Z_UNKNOWN_SP,
--                  '@' → -3 Z_UNKNOWN_USER, NULL → -2 Unassigned, else -1 Unknown)
-- query_tags (MAP) dropped — not consumable by Power BI directly.
-- Power BI relationships:
--   vd_pbi_workspace.workspace_id → vf_pbi_query_cost.workspace_id
--   vd_pbi_resource.resource_id   → vf_pbi_query_cost.resource_id
--   vd_pbi_identity.identity_id   → vf_pbi_query_cost.identity_id
CREATE OR REPLACE VIEW dataplatform_usage.vf_pbi_query_cost AS
SELECT
  t1.statement_cd,
  t1.usage_dt,
  t1.account_cd,
  COALESCE(t2.workspace_id, -3)                    AS workspace_id,
  COALESCE(t3.resource_id, -6)                     AS resource_id,
  COALESCE(
    t4.identity_id,
    CASE
      WHEN NULLIF(t1.executed_by_lb, '') IS NULL THEN 0                                        -- System-User (no identity)
      WHEN t1.executed_by_lb RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        THEN -4                                                                                -- Z_UNKNOWN_SP (GUID-shaped)
      WHEN t1.executed_by_lb LIKE '%@%' THEN -3                                                -- Z_UNKNOWN_USER (email-shaped)
      ELSE -1                                                                                  -- Unknown
    END
  )                                                 AS identity_id,
  t1.warehouse_cd,
  t1.executed_by_lb,
  t1.statement_type_lb,
  t1.execution_status_lb,
  COALESCE(NULLIF(t1.client_application_lb, ''), 'undefined') AS client_application_lb,
  t1.client_driver_lb,
  CASE WHEN t1.from_result_cache_fl = 1 THEN 'Yes' ELSE 'No' END AS from_result_cache_fl,
  t1.start_dt,
  t1.end_dt,
  t1.total_duration_ms_nb,
  t1.waiting_for_compute_ms_nb,
  t1.waiting_at_capacity_ms_nb,
  t1.compilation_duration_ms_nb,
  t1.execution_duration_ms_nb,
  t1.billable_duration_ms_nb,
  t1.read_rows_nb,
  t1.produced_rows_nb,
  ROUND(t1.read_bytes_nb    / 1e9, 4)              AS read_gb_nb,
  t1.written_rows_nb,
  ROUND(t1.written_bytes_nb / 1e9, 4)              AS written_gb_nb,
  t1.dashboard_cd,
  t1.notebook_cd,
  t1.job_cd,
  t1.genie_space_cd,
  t1.warehouse_cost_usd,
  t1.total_billable_ms_nb,
  t1.attributed_cost_usd
FROM global_it_hub.mvf_query_cost_attribution AS t1
LEFT JOIN global_it_hub.td_workspace AS t2
  ON  t2.account_cd      = t1.account_cd
  AND t2.workspace_cd = t1.workspace_cd
LEFT JOIN global_it_hub.td_resource AS t3
  ON  t3.account_cd       = t1.account_cd
  AND t3.workspace_cd  = t1.workspace_cd
  AND t3.resource_type_lb = 'WAREHOUSE'
  AND t3.source_cd     = t1.warehouse_cd
LEFT JOIN global_it_hub.td_identity AS t4
  ON  t4.account_cd     = t1.account_cd
  AND t4.identity_cd = t1.executed_by_lb;

-- COMMAND ----------

-- DBTITLE 1,vf_pbi_serving_cost (v0)
-- Fact view: one row per AI Gateway serving request with its allocated endpoint cost.
-- Source: mvf_serving_cost_attribution (MV owned by the attributed_cost DLT pipeline,
-- pipelines/silver/silver_serving_cost_attribution.sql — refreshed after silver_billing_cost).
-- v0: straightforward FK resolution mirroring vf_pbi_query_cost; revisit once this has been
-- validated against real serving usage (token-vs-request-count allocation split, entity_type_lb
-- coverage, etc.).
-- Integer FKs resolved like vf_pbi_billing_cost/vf_pbi_query_cost:
--   workspace_id → td_workspace (-3 = unknown workspace)
--   resource_id  → td_resource ENDPOINT rows (-9 = Z_UNKNOWN_ENDPOINT)
--   identity_id  → td_identity via requester_lb (fallback by shape: GUID → -4 Z_UNKNOWN_SP,
--                  '@' → -3 Z_UNKNOWN_USER, NULL → 0 System-User, else -1 Unknown)
--   entity_id    → td_serving_entity via served_entity_cd (-1 = Unknown; td_serving_entity only
--                  has the -2/-1 sentinel pair, unlike td_resource's per-type scheme, since
--                  every mvf_serving_cost_attribution row already comes from a join through
--                  td_serving_entity — a miss here should be rare/never)
-- Power BI relationships:
--   vd_pbi_workspace.workspace_id       → vf_pbi_serving_cost.workspace_id
--   vd_pbi_resource.resource_id         → vf_pbi_serving_cost.resource_id
--   vd_pbi_identity.identity_id         → vf_pbi_serving_cost.identity_id
--   vd_pbi_serving_entity.entity_id     → vf_pbi_serving_cost.entity_id
CREATE OR REPLACE VIEW dataplatform_usage.vf_pbi_serving_cost AS
SELECT
  t1.databricks_request_cd,
  t1.client_request_cd,
  t1.usage_dt,
  t1.account_cd,
  COALESCE(t2.workspace_id, -3)                    AS workspace_id,
  COALESCE(t3.resource_id, -9)                     AS resource_id,
  COALESCE(
    t4.identity_id,
    CASE
      WHEN NULLIF(t1.requester_lb, '') IS NULL THEN 0                                       -- System-User (no identity)
      WHEN t1.requester_lb RLIKE '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        THEN -4                                                                             -- Z_UNKNOWN_SP (GUID-shaped)
      WHEN t1.requester_lb LIKE '%@%' THEN -3                                               -- Z_UNKNOWN_USER (email-shaped)
      ELSE -1                                                                               -- Unknown
    END
  )                                                 AS identity_id,
  COALESCE(t5.entity_id, -1)                       AS entity_id,
  t1.served_entity_cd,
  t1.region_lb,
  t1.endpoint_cd,
  t1.endpoint_name_lb,
  t1.entity_type_lb,
  t1.entity_name_lb,
  t1.requester_lb,
  t1.status_code_nb,
  t1.request_dt,
  t1.input_token_count_nb,
  t1.output_token_count_nb,
  t1.input_character_count_nb,
  t1.output_character_count_nb,
  CASE WHEN t1.request_streaming_fl = 1 THEN 'Yes' ELSE 'No' END AS request_streaming_fl,
  t1.total_token_count_nb,
  t1.endpoint_cost_usd,
  t1.daily_total_token_count_nb,
  t1.daily_total_request_count_nb,
  t1.attributed_cost_usd
FROM global_it_hub.mvf_serving_cost_attribution AS t1
LEFT JOIN global_it_hub.td_workspace AS t2
  ON  t2.account_cd   = t1.account_cd
  AND t2.workspace_cd = t1.workspace_cd
LEFT JOIN global_it_hub.td_resource AS t3
  ON  t3.account_cd       = t1.account_cd
  AND t3.workspace_cd     = t1.workspace_cd
  AND t3.resource_type_lb = 'ENDPOINT'
  AND t3.source_cd        = t1.endpoint_cd
LEFT JOIN global_it_hub.td_identity AS t4
  ON  t4.account_cd  = t1.account_cd
  AND t4.identity_cd = t1.requester_lb
LEFT JOIN global_it_hub.td_serving_entity AS t5
  ON  t5.account_cd       = t1.account_cd
  AND t5.workspace_cd     = t1.workspace_cd
  AND t5.served_entity_cd = t1.served_entity_cd;
