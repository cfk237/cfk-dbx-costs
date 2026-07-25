-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Lakeflow Jobs (SCD2)
-- MAGIC
-- MAGIC Incrementally loads `system.lakeflow.jobs` into two silver tables:
-- MAGIC
-- MAGIC | Table | Purpose |
-- MAGIC |---|---|
-- MAGIC | `td_lakeflow_job_history` | Full SCD2 history — one row per job version, explicit validity windows |
-- MAGIC | `td_lakeflow_job` | Current state snapshot — one row per job, latest version only |
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC 1. Read the high-water-mark from `_pipeline_watermarks` (defaults to epoch on first run)
-- MAGIC 2. Identify job IDs with any `change_time > watermark`
-- MAGIC 3. Re-derive **all versions** of those jobs from source — required so the `LEAD()` window
-- MAGIC    correctly closes `valid_to_dt` on the previously-open record when a new version arrives
-- MAGIC 4. `MERGE` into history table on `(workspace_id_cd, job_id_cd, valid_from_dt)`
-- MAGIC 5. `MERGE` into current table on `(workspace_id_cd, job_id_cd)` sourced from `is_current_fl = 1` rows
-- MAGIC 6. Advance the watermark to `MAX(change_time)` of processed jobs
-- MAGIC
-- MAGIC The pattern is **idempotent**: re-running after a partial failure replays affected jobs with no net change.

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
WHERE table_name = 'global_it_hub.td_lakeflow_job_history'
  AND account_id  = '*';

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Identify affected jobs
-- Any job that has at least one new version since the last successful run.
-- We will re-derive ALL versions for these jobs in the next step.
CREATE OR REPLACE TEMPORARY TABLE v_affected_jobs AS
SELECT DISTINCT workspace_id AS workspace_id_cd, job_id
FROM databricks.sal_lakeflow_jobs
WHERE sal_load_dt > (SELECT last_processed_dt FROM v_watermark);

-- COMMAND ----------

-- DBTITLE 1,Step 3 — Compute SCD2 validity windows for affected jobs
-- Why pull all versions (not just new ones)?
-- The LEAD() function needs the full ordered partition to compute valid_to_dt.
-- Without earlier rows the previously-open record's window can't be closed correctly.
CREATE OR REPLACE TEMP VIEW v_scd2_source AS
SELECT
  t1.account_id,
  t1.workspace_id                                                             AS workspace_id_cd,
  t1.job_id                                                                   AS job_id_cd,
  t1.name                                                                     AS job_name_lb,
  t1.description                                                              AS description_lb,
  t1.creator_user_name                                                        AS creator_userid_cd,
  t1.create_time                                                              AS create_dt,
  t1.run_as                                                                   AS run_as_cd,
  t1.run_as_user_name                                                         AS run_as_user_lb,
  t1.tags,
  t1.change_time                                                              AS valid_from_dt,
  LEAD(t1.change_time) OVER (
    PARTITION BY t1.workspace_id, t1.job_id
    ORDER BY t1.change_time
  ) - INTERVAL 1 MICROSECOND                                                 AS valid_to_dt,
  CASE
    WHEN LEAD(t1.change_time) OVER (
           PARTITION BY t1.workspace_id, t1.job_id ORDER BY t1.change_time
         ) IS NULL THEN 1
    ELSE 0
  END                                                                        AS is_current_fl,
  CASE WHEN t1.delete_time IS NOT NULL THEN 1 ELSE 0 END                      AS is_deleted_fl,
  t1.delete_time                                                              AS delete_dt,
  CURRENT_TIMESTAMP()                                                        AS sys_update_dt
FROM databricks.sal_lakeflow_jobs AS t1
INNER JOIN v_affected_jobs AS t2
  ON  t1.workspace_id = t2.workspace_id_cd
  AND t1.job_id       = t2.job_id;

-- COMMAND ----------

-- DBTITLE 1,Step 4 — Upsert history table
-- Merge key: (account_id, workspace_id_cd, job_id_cd, valid_from_dt) — uniquely identifies a job version.
-- UPDATE: fires only when the previously-open record has its window closed
--         (valid_to_dt / is_current_fl changed). All other fields are immutable per version.
-- INSERT: new versions not yet present in silver.
MERGE INTO global_it_hub.td_lakeflow_job_history AS tgt
USING v_scd2_source                         AS src
  ON  tgt.account_id    = src.account_id
  AND tgt.workspace_id_cd  = src.workspace_id_cd
  AND tgt.job_id_cd     = src.job_id_cd
  AND tgt.valid_from_dt = src.valid_from_dt
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
  account_id, workspace_id_cd, job_id_cd,
  job_name_lb, description_lb, creator_userid_cd, create_dt, run_as_cd, run_as_user_lb, tags,
  valid_from_dt, valid_to_dt, is_current_fl, is_deleted_fl, delete_dt,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.job_id_cd,
  src.job_name_lb, src.description_lb, src.creator_userid_cd, src.create_dt, src.run_as_cd, src.run_as_user_lb, src.tags,
  src.valid_from_dt, src.valid_to_dt, src.is_current_fl, src.is_deleted_fl, src.delete_dt,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 5 — Upsert current table
-- Sourced from the history table (single source of truth) rather than re-reading source,
-- so the current table is always consistent with the history table.
-- Scoped to affected jobs only to keep the scan efficient.
MERGE INTO global_it_hub.td_lakeflow_job AS tgt
USING (
  SELECT *
  FROM   global_it_hub.td_lakeflow_job_history
  WHERE  is_current_fl = 1
    AND  job_id_cd IN (SELECT job_id FROM v_affected_jobs)
) AS src
  ON  tgt.account_id   = src.account_id
  AND tgt.workspace_id_cd = src.workspace_id_cd
  AND tgt.job_id_cd    = src.job_id_cd
WHEN MATCHED AND (
     tgt.job_name_lb       IS DISTINCT FROM src.job_name_lb
  OR tgt.description_lb    IS DISTINCT FROM src.description_lb
  OR tgt.creator_userid_cd IS DISTINCT FROM src.creator_userid_cd
  OR tgt.run_as_cd         IS DISTINCT FROM src.run_as_cd
  OR tgt.run_as_user_lb    IS DISTINCT FROM src.run_as_user_lb
  OR TO_JSON(tgt.tags)     IS DISTINCT FROM TO_JSON(src.tags)
  OR tgt.valid_from_dt     IS DISTINCT FROM src.valid_from_dt
  OR tgt.is_deleted_fl     IS DISTINCT FROM src.is_deleted_fl
) THEN UPDATE SET
  tgt.account_id         = src.account_id,
  tgt.job_name_lb        = src.job_name_lb,
  tgt.description_lb     = src.description_lb,
  tgt.creator_userid_cd  = src.creator_userid_cd,
  tgt.run_as_cd          = src.run_as_cd,
  tgt.run_as_user_lb     = src.run_as_user_lb,
  tgt.tags               = src.tags,
  tgt.valid_from_dt      = src.valid_from_dt,
  tgt.is_deleted_fl      = src.is_deleted_fl,
  tgt.delete_dt          = src.delete_dt,
  tgt.sys_update_dt      = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, workspace_id_cd, job_id_cd,
  job_name_lb, description_lb, creator_userid_cd, create_dt, run_as_cd, run_as_user_lb, tags,
  valid_from_dt, is_deleted_fl, delete_dt,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.job_id_cd,
  src.job_name_lb, src.description_lb, src.creator_userid_cd, src.create_dt, src.run_as_cd, src.run_as_user_lb, src.tags,
  src.valid_from_dt, src.is_deleted_fl, src.delete_dt,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 6 — Advance watermark
-- Written last so a partial failure leaves the watermark unchanged,
-- guaranteeing affected jobs are replayed on the next run.
MERGE INTO global_it_hub._pipeline_watermarks AS tgt
USING (
  SELECT
    'global_it_hub.td_lakeflow_job_history'         AS table_name,
    '*'                                              AS account_id,
    COALESCE(
      MAX(t1.sal_load_dt),
      (SELECT last_processed_dt FROM v_watermark)
    )                                               AS last_processed_dt,
    CURRENT_TIMESTAMP()                             AS sys_update_dt
  FROM databricks.sal_lakeflow_jobs AS t1
  INNER JOIN v_affected_jobs AS t2
    ON  t1.workspace_id = t2.workspace_id_cd
    AND t1.job_id       = t2.job_id
) AS src
  ON  tgt.table_name = src.table_name
  AND tgt.account_id  = src.account_id
WHEN MATCHED THEN UPDATE SET
  tgt.last_processed_dt = src.last_processed_dt,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (table_name, account_id, last_processed_dt, sys_update_dt)
  VALUES (src.table_name, src.account_id, src.last_processed_dt, src.sys_update_dt);
