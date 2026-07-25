-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Compute Warehouses (SCD2)
-- MAGIC
-- MAGIC Incrementally loads `system.compute.warehouses` into two silver tables:
-- MAGIC
-- MAGIC | Table | Purpose |
-- MAGIC |---|---|
-- MAGIC | `td_compute_warehouse_history` | Full SCD2 history — one row per warehouse version, explicit validity windows |
-- MAGIC | `td_compute_warehouse` | Current state snapshot — one row per warehouse, latest version only |
-- MAGIC
-- MAGIC Note: `system.compute.warehouses` has no `create_time` column — `create_dt` is therefore absent.
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC 1. Read the high-water-mark from `_pipeline_watermarks` (defaults to epoch on first run)
-- MAGIC 2. Identify warehouse IDs with any `change_time > watermark`
-- MAGIC 3. Re-derive **all versions** of those warehouses from source — required so the `LEAD()` window
-- MAGIC    correctly closes `valid_to_dt` on the previously-open record when a new version arrives
-- MAGIC 4. `MERGE` into history table on `(workspace_id_cd, warehouse_id_cd, valid_from_dt)`
-- MAGIC 5. `MERGE` into current table on `(workspace_id_cd, warehouse_id_cd)` sourced from `is_current_fl = 1` rows
-- MAGIC 6. Advance the watermark to `MAX(change_time)` of processed warehouses
-- MAGIC
-- MAGIC The pattern is **idempotent**: re-running after a partial failure replays affected warehouses with no net change.

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
WHERE table_name = 'global_it_hub.td_compute_warehouse_history'
  AND account_id  = '*';

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Identify affected warehouses
-- Any warehouse that has at least one new version since the last successful run.
-- We will re-derive ALL versions for these warehouses in the next step.
CREATE OR REPLACE TEMPORARY TABLE v_affected_warehouses AS
SELECT DISTINCT workspace_id AS workspace_id_cd, warehouse_id
FROM databricks.sal_compute_warehouses
WHERE sal_load_dt > (SELECT last_processed_dt FROM v_watermark);

-- COMMAND ----------

-- DBTITLE 1,Step 3 — Compute SCD2 validity windows for affected warehouses
-- Why pull all versions (not just new ones)?
-- The LEAD() function needs the full ordered partition to compute valid_to_dt.
-- Without earlier rows the previously-open record's window can't be closed correctly.
CREATE OR REPLACE TEMP VIEW v_scd2_source AS
SELECT
  t1.account_id,
  t1.workspace_id                                                             AS workspace_id_cd,
  t1.warehouse_id                                                              AS warehouse_id_cd,
  t1.warehouse_name                                                           AS warehouse_name_lb,
  t1.warehouse_type                                                           AS warehouse_type_lb,
  t1.warehouse_channel                                                        AS warehouse_channel_lb,
  t1.warehouse_size                                                           AS warehouse_size_lb,
  t1.min_clusters                                                             AS min_clusters_nb,
  t1.max_clusters                                                             AS max_clusters_nb,
  t1.auto_stop_minutes                                                        AS auto_stop_minutes_nb,
  t1.tags,
  t1.change_time                                                              AS valid_from_dt,
  LEAD(t1.change_time) OVER (
    PARTITION BY t1.workspace_id, t1.warehouse_id
    ORDER BY t1.change_time
  ) - INTERVAL 1 MICROSECOND                                                 AS valid_to_dt,
  CASE
    WHEN LEAD(t1.change_time) OVER (
           PARTITION BY t1.workspace_id, t1.warehouse_id ORDER BY t1.change_time
         ) IS NULL THEN 1
    ELSE 0
  END                                                                        AS is_current_fl,
  CASE WHEN t1.delete_time IS NOT NULL THEN 1 ELSE 0 END                     AS is_deleted_fl,
  t1.delete_time                                                              AS delete_dt,
  CURRENT_TIMESTAMP()                                                        AS sys_update_dt
FROM databricks.sal_compute_warehouses AS t1
INNER JOIN v_affected_warehouses AS t2
  ON  t1.workspace_id  = t2.workspace_id_cd
  AND t1.warehouse_id  = t2.warehouse_id;

-- COMMAND ----------

-- DBTITLE 1,Step 4 — Upsert history table
-- Merge key: (account_id, workspace_id_cd, warehouse_id_cd, valid_from_dt) — uniquely identifies a warehouse version.
-- UPDATE: fires only when the previously-open record has its window closed
--         (valid_to_dt / is_current_fl changed). All other fields are immutable per version.
-- INSERT: new versions not yet present in silver.
MERGE INTO global_it_hub.td_compute_warehouse_history AS tgt
USING v_scd2_source                                    AS src
  ON  tgt.account_id       = src.account_id
  AND tgt.workspace_id_cd     = src.workspace_id_cd
  AND tgt.warehouse_id_cd  = src.warehouse_id_cd
  AND tgt.valid_from_dt    = src.valid_from_dt
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
  account_id, workspace_id_cd, warehouse_id_cd,
  warehouse_name_lb, warehouse_type_lb, warehouse_channel_lb, warehouse_size_lb,
  min_clusters_nb, max_clusters_nb, auto_stop_minutes_nb, tags,
  valid_from_dt, valid_to_dt, is_current_fl, is_deleted_fl, delete_dt,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.warehouse_id_cd,
  src.warehouse_name_lb, src.warehouse_type_lb, src.warehouse_channel_lb, src.warehouse_size_lb,
  src.min_clusters_nb, src.max_clusters_nb, src.auto_stop_minutes_nb, src.tags,
  src.valid_from_dt, src.valid_to_dt, src.is_current_fl, src.is_deleted_fl, src.delete_dt,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 5 — Upsert current table
-- Sourced from the history table (single source of truth) rather than re-reading source,
-- so the current table is always consistent with the history table.
-- Scoped to affected warehouses only to keep the scan efficient.
MERGE INTO global_it_hub.td_compute_warehouse AS tgt
USING (
  SELECT *
  FROM   global_it_hub.td_compute_warehouse_history
  WHERE  is_current_fl = 1
    AND  warehouse_id_cd IN (SELECT warehouse_id FROM v_affected_warehouses)
) AS src
  ON  tgt.account_id      = src.account_id
  AND tgt.workspace_id_cd    = src.workspace_id_cd
  AND tgt.warehouse_id_cd = src.warehouse_id_cd
WHEN MATCHED AND (
     tgt.warehouse_name_lb    IS DISTINCT FROM src.warehouse_name_lb
  OR tgt.warehouse_type_lb    IS DISTINCT FROM src.warehouse_type_lb
  OR tgt.warehouse_channel_lb IS DISTINCT FROM src.warehouse_channel_lb
  OR tgt.warehouse_size_lb    IS DISTINCT FROM src.warehouse_size_lb
  OR tgt.min_clusters_nb      IS DISTINCT FROM src.min_clusters_nb
  OR tgt.max_clusters_nb      IS DISTINCT FROM src.max_clusters_nb
  OR tgt.auto_stop_minutes_nb IS DISTINCT FROM src.auto_stop_minutes_nb
  OR TO_JSON(tgt.tags)        IS DISTINCT FROM TO_JSON(src.tags)
  OR tgt.valid_from_dt        IS DISTINCT FROM src.valid_from_dt
  OR tgt.is_deleted_fl        IS DISTINCT FROM src.is_deleted_fl
) THEN UPDATE SET
  tgt.account_id             = src.account_id,
  tgt.warehouse_name_lb      = src.warehouse_name_lb,
  tgt.warehouse_type_lb      = src.warehouse_type_lb,
  tgt.warehouse_channel_lb   = src.warehouse_channel_lb,
  tgt.warehouse_size_lb      = src.warehouse_size_lb,
  tgt.min_clusters_nb        = src.min_clusters_nb,
  tgt.max_clusters_nb        = src.max_clusters_nb,
  tgt.auto_stop_minutes_nb   = src.auto_stop_minutes_nb,
  tgt.tags                   = src.tags,
  tgt.valid_from_dt          = src.valid_from_dt,
  tgt.is_deleted_fl          = src.is_deleted_fl,
  tgt.delete_dt              = src.delete_dt,
  tgt.sys_update_dt          = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, workspace_id_cd, warehouse_id_cd,
  warehouse_name_lb, warehouse_type_lb, warehouse_channel_lb, warehouse_size_lb,
  min_clusters_nb, max_clusters_nb, auto_stop_minutes_nb, tags,
  valid_from_dt, is_deleted_fl, delete_dt,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.warehouse_id_cd,
  src.warehouse_name_lb, src.warehouse_type_lb, src.warehouse_channel_lb, src.warehouse_size_lb,
  src.min_clusters_nb, src.max_clusters_nb, src.auto_stop_minutes_nb, src.tags,
  src.valid_from_dt, src.is_deleted_fl, src.delete_dt,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 6 — Advance watermark
-- Written last so a partial failure leaves the watermark unchanged,
-- guaranteeing affected warehouses are replayed on the next run.
MERGE INTO global_it_hub._pipeline_watermarks AS tgt
USING (
  SELECT
    'global_it_hub.td_compute_warehouse_history'    AS table_name,
    '*'                                              AS account_id,
    COALESCE(
      MAX(t1.sal_load_dt),
      (SELECT last_processed_dt FROM v_watermark)
    )                                               AS last_processed_dt,
    CURRENT_TIMESTAMP()                             AS sys_update_dt
  FROM databricks.sal_compute_warehouses AS t1
  INNER JOIN v_affected_warehouses AS t2
    ON  t1.workspace_id  = t2.workspace_id_cd
    AND t1.warehouse_id  = t2.warehouse_id
) AS src
  ON  tgt.table_name = src.table_name
  AND tgt.account_id  = src.account_id
WHEN MATCHED THEN UPDATE SET
  tgt.last_processed_dt = src.last_processed_dt,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (table_name, account_id, last_processed_dt, sys_update_dt)
  VALUES (src.table_name, src.account_id, src.last_processed_dt, src.sys_update_dt);
