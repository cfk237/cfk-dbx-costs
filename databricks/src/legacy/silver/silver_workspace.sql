-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Workspace Dimension (SCD1)
-- MAGIC
-- MAGIC Loads `databricks.sal_workspaces` into `global_it_hub.td_workspace`.
-- MAGIC
-- MAGIC | Source | Target |
-- MAGIC |---|---|
-- MAGIC | `databricks.sal_workspaces` | `global_it_hub.td_workspace` |
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC 1. Read the high-water-mark from `_pipeline_watermarks`
-- MAGIC 2. Identify workspace records with any new SAL load since the last run; deduplicate to
-- MAGIC    the latest snapshot per `(account_id, workspace_id)`
-- MAGIC 3. `MERGE` into `td_workspace` — UPDATE when name, URL, or status has changed, INSERT new workspaces
-- MAGIC 4. Advance the watermark to `MAX(sal_load_dt)` of processed records

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

-- COMMAND ----------

-- DBTITLE 1,Step 1 — Read watermark
CREATE OR REPLACE TEMP VIEW v_watermark AS
SELECT COALESCE(
  MAX(last_processed_dt),
  CAST('1970-01-01 00:00:00' AS TIMESTAMP)
) AS last_processed_dt
FROM global_it_hub._pipeline_watermarks
WHERE table_name = 'global_it_hub.td_workspace'
  AND account_id  = '*';

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Identify affected workspace records
-- Filters to SAL records newer than the watermark, then deduplicates to the latest
-- snapshot per workspace — SAL accumulates full snapshots on each extract run.
CREATE OR REPLACE TEMPORARY TABLE v_affected_workspaces AS
SELECT
  account_id,
  workspace_id                                           AS workspace_id_cd,
  workspace_name                                         AS workspace_name_lb,
  workspace_url                                          AS workspace_url_lb,
  create_time                                            AS create_dt,
  status                                                 AS status_lb,
  sal_load_dt,
  CURRENT_TIMESTAMP()                                    AS sys_update_dt
FROM databricks.sal_workspaces
WHERE sal_load_dt > (SELECT last_processed_dt FROM v_watermark)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY account_id, workspace_id
  ORDER BY sal_load_dt DESC
) = 1;

-- COMMAND ----------

-- DBTITLE 1,Step 3 — Upsert silver dimension
-- Merge key: (account_id, workspace_id_cd).
-- UPDATE fires when workspace_name, workspace_url, or status has changed.
-- create_dt is immutable — excluded from UPDATE SET, set on INSERT only.
MERGE INTO global_it_hub.td_workspace AS tgt
USING v_affected_workspaces            AS src
  ON  tgt.account_id      = src.account_id
  AND tgt.workspace_id_cd = src.workspace_id_cd
WHEN MATCHED AND (
     tgt.workspace_name_lb IS DISTINCT FROM src.workspace_name_lb
  OR tgt.workspace_url_lb  IS DISTINCT FROM src.workspace_url_lb
  OR tgt.status_lb         IS DISTINCT FROM src.status_lb
) THEN UPDATE SET
  tgt.workspace_name_lb = src.workspace_name_lb,
  tgt.workspace_url_lb  = src.workspace_url_lb,
  tgt.status_lb         = src.status_lb,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, workspace_id_cd, workspace_name_lb, workspace_url_lb, create_dt, status_lb,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.workspace_name_lb, src.workspace_url_lb, src.create_dt, src.status_lb,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 4 — Advance watermark
MERGE INTO global_it_hub._pipeline_watermarks AS tgt
USING (
  SELECT
    'global_it_hub.td_workspace'  AS table_name,
    '*'                           AS account_id,
    COALESCE(
      MAX(sal_load_dt),
      (SELECT last_processed_dt FROM v_watermark)
    )                             AS last_processed_dt,
    CURRENT_TIMESTAMP()           AS sys_update_dt
  FROM v_affected_workspaces
) AS src
  ON  tgt.table_name = src.table_name
  AND tgt.account_id  = src.account_id
WHEN MATCHED THEN UPDATE SET
  tgt.last_processed_dt = src.last_processed_dt,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (table_name, account_id, last_processed_dt, sys_update_dt)
  VALUES (src.table_name, src.account_id, src.last_processed_dt, src.sys_update_dt);
