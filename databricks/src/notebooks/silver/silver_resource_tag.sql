-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Resource Tag Bridge
-- MAGIC
-- MAGIC Builds `tr_resource_tag`: one row per `(resource_id, tag_key_id)` combining actual tag values
-- MAGIC with `'Untagged'` for tag keys the resource does not carry.
-- MAGIC
-- MAGIC ### Logic
-- MAGIC 1. **Stage actual tags** — explode the `tags MAP` from each source dimension into
-- MAGIC    `(resource_id, raw_key_lb, tag_value_lb)` rows via `LATERAL VIEW EXPLODE`
-- MAGIC 2. **Cross-join** every resource in `td_resource` with every key in `td_tag_key`
-- MAGIC 3. **Left-join** with actual tags on `raw_key_lb` — normalization lives exclusively in
-- MAGIC    `td_tag_key.raw_key_lb`; no CASE logic duplicated here
-- MAGIC 4. **MERGE** into `tr_resource_tag` on `(resource_id, tag_key_id)` — integer FK pair,
-- MAGIC    optimal for Power BI VertiPaq relationships
-- MAGIC    - `UPDATE` when the tag value has changed
-- MAGIC    - `INSERT` for new (resource, tag key) combinations
-- MAGIC
-- MAGIC **Prerequisites**: `silver_resource`, `silver_tag_key`, all four SCD2 silver notebooks, and
-- MAGIC the billing usage load (`silver_billing_usage` / DLT `silver_billing`) must have run first so
-- MAGIC `td_resource`, `td_tag_key`, and the source dimensions (including the four billing dimensions)
-- MAGIC are populated.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog_env", "dev")
-- MAGIC catalog_env = dbutils.widgets.get("catalog_env")
-- MAGIC
-- MAGIC dbutils.widgets.text("catalog_name", "it_security")
-- MAGIC catalog_name = dbutils.widgets.get("catalog_name")
-- MAGIC
-- MAGIC catalog = f"{catalog_name}_{catalog_env}"
-- MAGIC silver_schema = "global_it_hub"
-- MAGIC
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC spark.sql(f"USE SCHEMA {silver_schema}")
-- MAGIC
-- MAGIC print(f"Catalog : {catalog}")
-- MAGIC print(f"Schema  : {silver_schema}")

-- COMMAND ----------

-- DBTITLE 1,Step 1 — Stage actual tag values per resource
-- Explodes the tags MAP from each source dimension into (resource_id, raw_key_lb, tag_value_lb).
-- raw_key_lb is the MAP key exactly as stored (e.g. 'app_id') — normalization is NOT applied here.
-- The CROSS JOIN in Step 2 matches raw_key_lb to td_tag_key.raw_key_lb, which owns the mapping.
CREATE OR REPLACE TEMP VIEW v_resource_actual_tags AS

  -- Jobs
  SELECT t1.resource_id, raw_key_lb, tag_value_lb
  FROM global_it_hub.td_resource AS t1
  JOIN global_it_hub.td_lakeflow_job AS t2
    ON  t1.account_cd       = t2.account_cd
    AND t1.workspace_cd     = t2.workspace_cd
    AND t1.source_cd     = t2.job_cd
    AND t1.resource_type_lb = 'JOB'
  LATERAL VIEW EXPLODE(t2.tags) exploded AS raw_key_lb, tag_value_lb
  WHERE t1.resource_id > 0 AND t2.job_id > 0 AND t2.tags IS NOT NULL

  UNION ALL

  -- Warehouses
  SELECT t1.resource_id, raw_key_lb, tag_value_lb
  FROM global_it_hub.td_resource AS t1
  JOIN global_it_hub.td_compute_warehouse AS t2
    ON  t1.account_cd       = t2.account_cd
    AND t1.workspace_cd     = t2.workspace_cd
    AND t1.source_cd     = t2.warehouse_cd
    AND t1.resource_type_lb = 'WAREHOUSE'
  LATERAL VIEW EXPLODE(t2.tags) exploded AS raw_key_lb, tag_value_lb
  WHERE t1.resource_id > 0 AND t2.warehouse_id > 0 AND t2.tags IS NOT NULL

  UNION ALL

  -- Pipelines
  SELECT t1.resource_id, raw_key_lb, tag_value_lb
  FROM global_it_hub.td_resource AS t1
  JOIN global_it_hub.td_lakeflow_pipeline AS t2
    ON  t1.account_cd       = t2.account_cd
    AND t1.workspace_cd     = t2.workspace_cd
    AND t1.source_cd     = t2.pipeline_cd
    AND t1.resource_type_lb = 'PIPELINE'
  LATERAL VIEW EXPLODE(t2.tags) exploded AS raw_key_lb, tag_value_lb
  WHERE t1.resource_id > 0 AND t2.pipeline_id > 0 AND t2.tags IS NOT NULL

  UNION ALL

  -- Clusters
  SELECT t1.resource_id, raw_key_lb, tag_value_lb
  FROM global_it_hub.td_resource AS t1
  JOIN global_it_hub.td_compute_cluster AS t2
    ON  t1.account_cd       = t2.account_cd
    AND t1.workspace_cd     = t2.workspace_cd
    AND t1.source_cd     = t2.cluster_cd
    AND t1.resource_type_lb = 'CLUSTER'
  LATERAL VIEW EXPLODE(t2.tags) exploded AS raw_key_lb, tag_value_lb
  WHERE t1.resource_id > 0 AND t2.cluster_id > 0 AND t2.tags IS NOT NULL

  UNION ALL

  -- Billing — Apps
  SELECT t1.resource_id, raw_key_lb, tag_value_lb
  FROM global_it_hub.td_resource AS t1
  JOIN global_it_hub.td_billing_app AS t2
    ON  t1.account_cd       = t2.account_cd
    AND t1.workspace_cd     = t2.workspace_cd
    AND t1.source_cd     = t2.app_cd
    AND t1.resource_type_lb = t2.product_lb
  LATERAL VIEW EXPLODE(t2.tags) exploded AS raw_key_lb, tag_value_lb
  WHERE t1.resource_id > 0 AND t2.app_id > 0 AND t2.tags IS NOT NULL

  UNION ALL

  -- Billing — Notebooks
  SELECT t1.resource_id, raw_key_lb, tag_value_lb
  FROM global_it_hub.td_resource AS t1
  JOIN global_it_hub.td_billing_notebook AS t2
    ON  t1.account_cd       = t2.account_cd
    AND t1.workspace_cd     = t2.workspace_cd
    AND t1.source_cd     = t2.notebook_cd
    AND t1.resource_type_lb = t2.product_lb
  LATERAL VIEW EXPLODE(t2.tags) exploded AS raw_key_lb, tag_value_lb
  WHERE t1.resource_id > 0 AND t2.notebook_id > 0 AND t2.tags IS NOT NULL

  UNION ALL

  -- Billing — Endpoints
  SELECT t1.resource_id, raw_key_lb, tag_value_lb
  FROM global_it_hub.td_resource AS t1
  JOIN global_it_hub.td_billing_endpoint AS t2
    ON  t1.account_cd       = t2.account_cd
    AND t1.workspace_cd     = t2.workspace_cd
    AND t1.source_cd     = t2.endpoint_cd
    AND t1.resource_type_lb = t2.product_lb
  LATERAL VIEW EXPLODE(t2.tags) exploded AS raw_key_lb, tag_value_lb
  WHERE t1.resource_id > 0 AND t2.endpoint_id > 0 AND t2.tags IS NOT NULL

  UNION ALL

  -- Billing — Networks
  SELECT t1.resource_id, raw_key_lb, tag_value_lb
  FROM global_it_hub.td_resource AS t1
  JOIN global_it_hub.td_billing_network AS t2
    ON  t1.account_cd       = t2.account_cd
    AND t1.workspace_cd     = t2.workspace_cd
    AND t1.source_cd     = t2.network_client_cd
    AND t1.resource_type_lb = 'NETWORKING'
  LATERAL VIEW EXPLODE(t2.tags) exploded AS raw_key_lb, tag_value_lb
  WHERE t1.resource_id > 0 AND t2.network_id > 0 AND t2.tags IS NOT NULL;

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Merge resource × tag key combinations
-- CROSS JOIN td_resource × td_tag_key produces every (resource, key) slot.
-- LEFT JOIN on raw_key_lb resolves actual values — normalization mapping is in td_tag_key only.
-- COALESCE ensures 'Untagged' for every slot the resource does not carry.
-- No resource_type_lb filter — v_resource_actual_tags now covers all resource types in
-- td_resource (jobs, warehouses, pipelines, clusters, and the four billing dimensions).
-- Merge key: (resource_id, tag_key_id) — two integer surrogates, optimal for Power BI joins.
MERGE INTO global_it_hub.tr_resource_tag AS tgt
USING (
  SELECT
    t1.resource_id,
    t2.tag_key_id,
    COALESCE(t3.tag_value_lb, 'Untagged')  AS tag_value_lb,
    CURRENT_TIMESTAMP()                     AS sys_update_dt
  FROM global_it_hub.td_resource AS t1
  CROSS JOIN global_it_hub.td_tag_key AS t2
  LEFT JOIN v_resource_actual_tags AS t3
    ON  t3.resource_id = t1.resource_id
    AND t3.raw_key_lb  = t2.raw_key_lb
  WHERE t1.resource_id > 0
    AND t2.tag_key_id  > 0
) AS src
  ON  tgt.resource_id = src.resource_id
  AND tgt.tag_key_id  = src.tag_key_id
WHEN MATCHED AND tgt.tag_value_lb IS DISTINCT FROM src.tag_value_lb
THEN UPDATE SET
  tgt.tag_value_lb  = src.tag_value_lb,
  tgt.sys_update_dt = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (resource_id, tag_key_id, tag_value_lb, sys_create_dt, sys_update_dt)
VALUES (src.resource_id, src.tag_key_id, src.tag_value_lb, src.sys_update_dt, src.sys_update_dt);
