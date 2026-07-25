-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Unified Resource Dimension
-- MAGIC
-- MAGIC Incrementally merges eight silver dimension tables into `td_resource`.
-- MAGIC Mirrors `td_identity` (users + service principals) for compute/workload entities.
-- MAGIC
-- MAGIC | Source | resource_type_lb | display_name_lb | owned_by_cd |
-- MAGIC |---|---|---|---|
-- MAGIC | `td_lakeflow_job` | `JOB` | `job_name_lb` | `creator_userid_cd` |
-- MAGIC | `td_compute_warehouse` | `WAREHOUSE` | `warehouse_name_lb` | — |
-- MAGIC | `td_lakeflow_pipeline` | `PIPELINE` | `pipeline_name_lb` | `created_by_cd` |
-- MAGIC | `td_compute_cluster` | `CLUSTER` | `cluster_name_lb` | `owned_by_cd` |
-- MAGIC | `td_billing_app` | `APP` | `app_name_lb` | `owned_by_cd` |
-- MAGIC | `td_billing_notebook` | `NOTEBOOK` | `notebook_path_lb` | — |
-- MAGIC | `td_billing_endpoint` | `product_lb` | `endpoint_name_lb` | — |
-- MAGIC | `td_billing_network` | `NETWORKING` | `network_client_lb` | — |
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC 1. Read watermark from `_pipeline_watermarks` (epoch on first run → full load)
-- MAGIC 2. Merge into `td_resource` — each source branch filtered by `sys_update_dt > watermark`
-- MAGIC    so only records touched by upstream notebooks during this run are processed
-- MAGIC 3. Advance watermark to `CURRENT_TIMESTAMP()` — next run skips unchanged records
-- MAGIC
-- MAGIC The pattern is **idempotent**: a failure in Step 2 leaves the watermark unchanged,
-- MAGIC causing the next run to replay all affected records with no net change.

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

-- DBTITLE 1,Step 1 — Read watermark
-- Defaults to epoch (1970-01-01) on first run, triggering a full load of all source dimensions.
CREATE OR REPLACE TEMP VIEW v_watermark AS
SELECT COALESCE(
  MAX(last_processed_dt),
  CAST('1970-01-01 00:00:00' AS TIMESTAMP)
) AS last_processed_dt
FROM global_it_hub._pipeline_watermarks
WHERE table_name = 'global_it_hub.td_resource'
  AND account_id  = '*';

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Upsert td_resource
-- Each source branch is scoped to sys_update_dt > watermark — only records written
-- by upstream SCD2 or billing notebooks during the current run are included.
-- Merge key: (account_id, workspace_id_cd, source_id_cd, resource_type_lb).
-- UPDATE fires when display_name_lb, owned_by_cd, or is_active_fl has changed.
MERGE INTO global_it_hub.td_resource AS tgt
USING (

  -- Jobs
  SELECT
    'JOB'                                                AS resource_type_lb,
    j.job_id_cd                                          AS source_id_cd,
    j.account_id,
    j.workspace_id_cd,
    j.job_name_lb                                        AS display_name_lb,
    j.creator_userid_cd                                  AS owned_by_cd,
    CASE WHEN j.is_deleted_fl = 0 THEN 1 ELSE 0 END     AS is_active_fl,
    CURRENT_TIMESTAMP()                                  AS sys_update_dt
  FROM global_it_hub.td_lakeflow_job AS j
  WHERE j.job_id > 0
    AND j.sys_update_dt > (SELECT last_processed_dt FROM v_watermark)

  UNION ALL

  -- Warehouses
  SELECT
    'WAREHOUSE'                                          AS resource_type_lb,
    w.warehouse_id_cd                                    AS source_id_cd,
    w.account_id,
    w.workspace_id_cd,
    w.warehouse_name_lb                                  AS display_name_lb,
    NULL                                                 AS owned_by_cd,
    CASE WHEN w.is_deleted_fl = 0 THEN 1 ELSE 0 END     AS is_active_fl,
    CURRENT_TIMESTAMP()                                  AS sys_update_dt
  FROM global_it_hub.td_compute_warehouse AS w
  WHERE w.warehouse_id > 0
    AND w.sys_update_dt > (SELECT last_processed_dt FROM v_watermark)

  UNION ALL

  -- Pipelines
  SELECT
    'PIPELINE'                                           AS resource_type_lb,
    p.pipeline_id_cd                                     AS source_id_cd,
    p.account_id,
    p.workspace_id_cd,
    p.pipeline_name_lb                                   AS display_name_lb,
    p.created_by_cd                                      AS owned_by_cd,
    CASE WHEN p.is_deleted_fl = 0 THEN 1 ELSE 0 END     AS is_active_fl,
    CURRENT_TIMESTAMP()                                  AS sys_update_dt
  FROM global_it_hub.td_lakeflow_pipeline AS p
  WHERE p.pipeline_id > 0
    AND p.sys_update_dt > (SELECT last_processed_dt FROM v_watermark)

  UNION ALL

  -- Clusters
  SELECT
    'CLUSTER'                                            AS resource_type_lb,
    c.cluster_id_cd                                      AS source_id_cd,
    c.account_id,
    c.workspace_id_cd,
    c.cluster_name_lb                                    AS display_name_lb,
    c.owned_by_cd,
    CASE WHEN c.is_deleted_fl = 0 THEN 1 ELSE 0 END     AS is_active_fl,
    CURRENT_TIMESTAMP()                                  AS sys_update_dt
  FROM global_it_hub.td_compute_cluster AS c
  WHERE c.cluster_id > 0
    AND c.sys_update_dt > (SELECT last_processed_dt FROM v_watermark)

  UNION ALL

  -- Apps
  SELECT
    'APP'                                                AS resource_type_lb,
    a.app_id_cd                                          AS source_id_cd,
    a.account_id,
    a.workspace_id_cd,
    a.app_name_lb                                        AS display_name_lb,
    a.owned_by_cd,
    1                                                    AS is_active_fl,
    CURRENT_TIMESTAMP()                                  AS sys_update_dt
  FROM global_it_hub.td_billing_app AS a
  WHERE a.app_id > 0
    AND a.sys_update_dt > (SELECT last_processed_dt FROM v_watermark)

  UNION ALL

  -- Notebooks
  SELECT
    'NOTEBOOK'                                           AS resource_type_lb,
    n.notebook_id_cd                                     AS source_id_cd,
    n.account_id,
    n.workspace_id_cd,
    n.notebook_path_lb                                   AS display_name_lb,
    NULL                                                 AS owned_by_cd,
    1                                                    AS is_active_fl,
    CURRENT_TIMESTAMP()                                  AS sys_update_dt
  FROM global_it_hub.td_billing_notebook AS n
  WHERE n.notebook_id > 0
    AND n.sys_update_dt > (SELECT last_processed_dt FROM v_watermark)

  UNION ALL

  -- Endpoints (resource_type_lb = product_lb for finer-grained typing, e.g. MODEL_SERVING)
  SELECT
    e.product_lb                                         AS resource_type_lb,
    e.endpoint_id_cd                                     AS source_id_cd,
    e.account_id,
    e.workspace_id_cd,
    e.endpoint_name_lb                                   AS display_name_lb,
    NULL                                                 AS owned_by_cd,
    1                                                    AS is_active_fl,
    CURRENT_TIMESTAMP()                                  AS sys_update_dt
  FROM global_it_hub.td_billing_endpoint AS e
  WHERE e.endpoint_id > 0
    AND e.sys_update_dt > (SELECT last_processed_dt FROM v_watermark)

  UNION ALL

  -- Network clients
  SELECT
    'NETWORKING'                                         AS resource_type_lb,
    n.network_client_lb                                  AS source_id_cd,
    n.account_id,
    n.workspace_id_cd,
    n.network_client_lb                                  AS display_name_lb,
    NULL                                                 AS owned_by_cd,
    1                                                    AS is_active_fl,
    CURRENT_TIMESTAMP()                                  AS sys_update_dt
  FROM global_it_hub.td_billing_network AS n
  WHERE n.network_id > 0
    AND n.sys_update_dt > (SELECT last_processed_dt FROM v_watermark)

) AS src
  ON  tgt.account_id       = src.account_id
  AND tgt.workspace_id_cd     = src.workspace_id_cd
  AND tgt.source_id_cd     = src.source_id_cd
  AND tgt.resource_type_lb = src.resource_type_lb
WHEN MATCHED AND (
     tgt.display_name_lb IS DISTINCT FROM src.display_name_lb
  OR tgt.owned_by_cd     IS DISTINCT FROM src.owned_by_cd
  OR tgt.is_active_fl    IS DISTINCT FROM src.is_active_fl
) THEN UPDATE SET
  tgt.display_name_lb = src.display_name_lb,
  tgt.owned_by_cd     = src.owned_by_cd,
  tgt.is_active_fl    = src.is_active_fl,
  tgt.sys_update_dt   = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  resource_type_lb, source_id_cd, account_id, workspace_id_cd,
  display_name_lb, owned_by_cd, is_active_fl,
  sys_create_dt, sys_update_dt
) VALUES (
  src.resource_type_lb, src.source_id_cd, src.account_id, src.workspace_id_cd,
  src.display_name_lb, src.owned_by_cd, src.is_active_fl,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 3 — Advance watermark
-- Written last so a failure in Step 2 leaves the watermark unchanged,
-- guaranteeing affected records are replayed on the next run.
MERGE INTO global_it_hub._pipeline_watermarks AS tgt
USING (
  SELECT
    'global_it_hub.td_resource' AS table_name,
    '*'                          AS account_id,
    CURRENT_TIMESTAMP()         AS last_processed_dt,
    CURRENT_TIMESTAMP()         AS sys_update_dt
) AS src
  ON  tgt.table_name = src.table_name
  AND tgt.account_id  = src.account_id
WHEN MATCHED THEN UPDATE SET
  tgt.last_processed_dt = src.last_processed_dt,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (table_name, account_id, last_processed_dt, sys_update_dt)
  VALUES (src.table_name, src.account_id, src.last_processed_dt, src.sys_update_dt);
