-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Lakeflow Pipelines (SCD2)
-- MAGIC
-- MAGIC Incrementally loads `system.lakeflow.pipelines` into two silver tables:
-- MAGIC
-- MAGIC | Table | Purpose |
-- MAGIC |---|---|
-- MAGIC | `td_lakeflow_pipeline_history` | Full SCD2 history — one row per pipeline version, explicit validity windows |
-- MAGIC | `td_lakeflow_pipeline` | Current state snapshot — one row per pipeline, latest version only |
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC 1. Read the high-water-mark from `_pipeline_watermarks` (defaults to epoch on first run)
-- MAGIC 2. Identify pipeline IDs with any `change_time > watermark`
-- MAGIC 3. Re-derive **all versions** of those pipelines from source — required so the `LEAD()` window
-- MAGIC    correctly closes `valid_to_dt` on the previously-open record when a new version arrives
-- MAGIC 4. `MERGE` into history table on `(workspace_id_cd, pipeline_id_cd, valid_from_dt)`
-- MAGIC 5. `MERGE` into current table on `(workspace_id_cd, pipeline_id_cd)` sourced from `is_current_fl = 1` rows
-- MAGIC 6. Advance the watermark to `MAX(change_time)` of processed pipelines
-- MAGIC
-- MAGIC The pattern is **idempotent**: re-running after a partial failure replays affected pipelines with no net change.

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
-- Defaults to epoch (1970-01-01) on first run, triggering a full historical load.
CREATE OR REPLACE TEMP VIEW v_watermark AS
SELECT COALESCE(
  MAX(last_processed_dt),
  CAST('1970-01-01 00:00:00' AS TIMESTAMP)
) AS last_processed_dt
FROM global_it_hub._pipeline_watermarks
WHERE table_name = 'global_it_hub.td_lakeflow_pipeline_history'
  AND account_id  = '*';

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Identify affected pipelines
-- Any pipeline that has at least one new version since the last successful run.
-- We will re-derive ALL versions for these pipelines in the next step.
CREATE OR REPLACE TEMPORARY TABLE v_affected_pipelines AS
SELECT DISTINCT workspace_id AS workspace_id_cd, pipeline_id
FROM databricks.sal_lakeflow_pipelines
WHERE sal_load_dt > (SELECT last_processed_dt FROM v_watermark);

-- COMMAND ----------

-- DBTITLE 1,Step 3 — Compute SCD2 validity windows for affected pipelines
-- Why pull all versions (not just new ones)?
-- The LEAD() function needs the full ordered partition to compute valid_to_dt.
-- Without earlier rows the previously-open record's window can't be closed correctly.
CREATE OR REPLACE TEMP VIEW v_scd2_source AS
SELECT
  t1.account_id,
  t1.workspace_id                                                             AS workspace_id_cd,
  t1.pipeline_id                                                               AS pipeline_id_cd,
  t1.pipeline_type                                                            AS pipeline_type_lb,
  t1.name                                                                     AS pipeline_name_lb,
  t1.created_by                                                               AS created_by_cd,
  t1.create_time                                                              AS create_dt,
  t1.run_as                                                                   AS run_as_cd,
  t1.tags,
  t1.configuration,
  t1.change_time                                                              AS valid_from_dt,
  LEAD(t1.change_time) OVER (
    PARTITION BY t1.workspace_id, t1.pipeline_id
    ORDER BY t1.change_time
  ) - INTERVAL 1 MICROSECOND                                                 AS valid_to_dt,
  CASE
    WHEN LEAD(t1.change_time) OVER (
           PARTITION BY t1.workspace_id, t1.pipeline_id ORDER BY t1.change_time
         ) IS NULL THEN 1
    ELSE 0
  END                                                                        AS is_current_fl,
  CASE WHEN t1.delete_time IS NOT NULL THEN 1 ELSE 0 END                     AS is_deleted_fl,
  t1.delete_time                                                              AS delete_dt,
  CURRENT_TIMESTAMP()                                                        AS sys_update_dt
FROM databricks.sal_lakeflow_pipelines AS t1
INNER JOIN v_affected_pipelines AS t2
  ON  t1.workspace_id  = t2.workspace_id_cd
  AND t1.pipeline_id   = t2.pipeline_id;

-- COMMAND ----------

-- DBTITLE 1,Step 4 — Upsert history table
-- Merge key: (account_id, workspace_id_cd, pipeline_id_cd, valid_from_dt) — uniquely identifies a pipeline version.
-- UPDATE: fires only when the previously-open record has its window closed
--         (valid_to_dt / is_current_fl changed). All other fields are immutable per version.
-- INSERT: new versions not yet present in silver.
MERGE INTO global_it_hub.td_lakeflow_pipeline_history AS tgt
USING v_scd2_source                                    AS src
  ON  tgt.account_id      = src.account_id
  AND tgt.workspace_id_cd    = src.workspace_id_cd
  AND tgt.pipeline_id_cd  = src.pipeline_id_cd
  AND tgt.valid_from_dt   = src.valid_from_dt
WHEN MATCHED AND (
     tgt.valid_to_dt   IS DISTINCT FROM src.valid_to_dt
  OR tgt.is_current_fl IS DISTINCT FROM src.is_current_fl
) THEN UPDATE SET
  tgt.valid_to_dt      = src.valid_to_dt,
  tgt.is_current_fl    = src.is_current_fl,
  tgt.is_deleted_fl    = src.is_deleted_fl,
  tgt.delete_dt        = src.delete_dt,
  tgt.sys_update_dt    = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, workspace_id_cd, pipeline_id_cd,
  pipeline_type_lb, pipeline_name_lb, created_by_cd, create_dt, run_as_cd, tags, configuration,
  valid_from_dt, valid_to_dt, is_current_fl, is_deleted_fl, delete_dt,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.pipeline_id_cd,
  src.pipeline_type_lb, src.pipeline_name_lb, src.created_by_cd, src.create_dt, src.run_as_cd, src.tags, src.configuration,
  src.valid_from_dt, src.valid_to_dt, src.is_current_fl, src.is_deleted_fl, src.delete_dt,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 5 — Upsert current table
-- Sourced from the history table (single source of truth) rather than re-reading source,
-- so the current table is always consistent with the history table.
-- Scoped to affected pipelines only to keep the scan efficient.
MERGE INTO global_it_hub.td_lakeflow_pipeline AS tgt
USING (
  SELECT *
  FROM   global_it_hub.td_lakeflow_pipeline_history
  WHERE  is_current_fl = 1
    AND  pipeline_id_cd IN (SELECT pipeline_id FROM v_affected_pipelines)
) AS src
  ON  tgt.account_id     = src.account_id
  AND tgt.workspace_id_cd   = src.workspace_id_cd
  AND tgt.pipeline_id_cd = src.pipeline_id_cd
WHEN MATCHED AND (
     tgt.pipeline_type_lb           IS DISTINCT FROM src.pipeline_type_lb
  OR tgt.pipeline_name_lb           IS DISTINCT FROM src.pipeline_name_lb
  OR tgt.created_by_cd              IS DISTINCT FROM src.created_by_cd
  OR tgt.run_as_cd                  IS DISTINCT FROM src.run_as_cd
  OR TO_JSON(tgt.tags)              IS DISTINCT FROM TO_JSON(src.tags)
  OR TO_JSON(tgt.configuration)     IS DISTINCT FROM TO_JSON(src.configuration)
  OR tgt.valid_from_dt              IS DISTINCT FROM src.valid_from_dt
  OR tgt.is_deleted_fl              IS DISTINCT FROM src.is_deleted_fl
) THEN UPDATE SET
  tgt.account_id          = src.account_id,
  tgt.pipeline_type_lb    = src.pipeline_type_lb,
  tgt.pipeline_name_lb    = src.pipeline_name_lb,
  tgt.created_by_cd       = src.created_by_cd,
  tgt.run_as_cd           = src.run_as_cd,
  tgt.tags                = src.tags,
  tgt.configuration       = src.configuration,
  tgt.valid_from_dt       = src.valid_from_dt,
  tgt.is_deleted_fl       = src.is_deleted_fl,
  tgt.delete_dt           = src.delete_dt,
  tgt.sys_update_dt       = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, workspace_id_cd, pipeline_id_cd,
  pipeline_type_lb, pipeline_name_lb, created_by_cd, create_dt, run_as_cd, tags, configuration,
  valid_from_dt, is_deleted_fl, delete_dt,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.pipeline_id_cd,
  src.pipeline_type_lb, src.pipeline_name_lb, src.created_by_cd, src.create_dt, src.run_as_cd, src.tags, src.configuration,
  src.valid_from_dt, src.is_deleted_fl, src.delete_dt,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 6 — Advance watermark
-- Written last so a partial failure leaves the watermark unchanged,
-- guaranteeing affected pipelines are replayed on the next run.
MERGE INTO global_it_hub._pipeline_watermarks AS tgt
USING (
  SELECT
    'global_it_hub.td_lakeflow_pipeline_history'    AS table_name,
    '*'                                              AS account_id,
    COALESCE(
      MAX(t1.sal_load_dt),
      (SELECT last_processed_dt FROM v_watermark)
    )                                               AS last_processed_dt,
    CURRENT_TIMESTAMP()                             AS sys_update_dt
  FROM databricks.sal_lakeflow_pipelines AS t1
  INNER JOIN v_affected_pipelines AS t2
    ON  t1.workspace_id = t2.workspace_id_cd
    AND t1.pipeline_id  = t2.pipeline_id
) AS src
  ON  tgt.table_name = src.table_name
  AND tgt.account_id  = src.account_id
WHEN MATCHED THEN UPDATE SET
  tgt.last_processed_dt = src.last_processed_dt,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (table_name, account_id, last_processed_dt, sys_update_dt)
  VALUES (src.table_name, src.account_id, src.last_processed_dt, src.sys_update_dt);
