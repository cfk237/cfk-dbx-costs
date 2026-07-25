-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Compute Clusters (SCD2)
-- MAGIC
-- MAGIC Incrementally loads `system.compute.clusters` into two silver tables:
-- MAGIC
-- MAGIC | Table | Purpose |
-- MAGIC |---|---|
-- MAGIC | `td_compute_cluster_history` | Full SCD2 history — one row per cluster version, explicit validity windows |
-- MAGIC | `td_compute_cluster` | Current state snapshot — one row per cluster, latest version only |
-- MAGIC
-- MAGIC Cloud-specific structs (`aws_attributes`, `azure_attributes`, `gcp_attributes`) and
-- MAGIC `init_scripts` are excluded — they are available in the source table for ad-hoc queries.
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC 1. Read the high-water-mark from `_pipeline_watermarks` (defaults to epoch on first run)
-- MAGIC 2. Identify cluster IDs with any `change_time > watermark`
-- MAGIC 3. Re-derive **all versions** of those clusters from source — required so the `LEAD()` window
-- MAGIC    correctly closes `valid_to_dt` on the previously-open record when a new version arrives
-- MAGIC 4. `MERGE` into history table on `(workspace_id_cd, cluster_id_cd, valid_from_dt)`
-- MAGIC 5. `MERGE` into current table on `(workspace_id_cd, cluster_id_cd)` sourced from `is_current_fl = 1` rows
-- MAGIC 6. Advance the watermark to `MAX(change_time)` of processed clusters
-- MAGIC
-- MAGIC The pattern is **idempotent**: re-running after a partial failure replays affected clusters with no net change.

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
WHERE table_name = 'global_it_hub.td_compute_cluster_history'
  AND account_id  = '*';

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Identify affected clusters
-- Any cluster that has at least one new version since the last successful run.
-- We will re-derive ALL versions for these clusters in the next step.
CREATE OR REPLACE TEMPORARY TABLE v_affected_clusters AS
SELECT DISTINCT workspace_id AS workspace_id_cd, cluster_id
FROM databricks.sal_compute_clusters
WHERE sal_load_dt > (SELECT last_processed_dt FROM v_watermark);

-- COMMAND ----------

-- DBTITLE 1,Step 3 — Compute SCD2 validity windows for affected clusters
-- Why pull all versions (not just new ones)?
-- The LEAD() function needs the full ordered partition to compute valid_to_dt.
-- Without earlier rows the previously-open record's window can't be closed correctly.
CREATE OR REPLACE TEMP VIEW v_scd2_source AS
SELECT
  t1.account_id,
  t1.workspace_id                                                             AS workspace_id_cd,
  t1.cluster_id                                                               AS cluster_id_cd,
  t1.cluster_name                                                             AS cluster_name_lb,
  t1.owned_by                                                                 AS owned_by_cd,
  t1.create_time                                                              AS create_dt,
  t1.cluster_source                                                           AS cluster_source_lb,
  t1.dbr_version                                                              AS dbr_version_lb,
  t1.driver_node_type                                                         AS driver_node_type_lb,
  t1.worker_node_type                                                         AS worker_node_type_lb,
  t1.worker_count                                                             AS worker_count_nb,
  t1.min_autoscale_workers                                                    AS min_autoscale_workers_nb,
  t1.max_autoscale_workers                                                    AS max_autoscale_workers_nb,
  t1.auto_termination_minutes                                                 AS auto_termination_minutes_nb,
  CASE WHEN t1.enable_elastic_disk THEN 1 ELSE 0 END                          AS enable_elastic_disk_fl,
  t1.driver_instance_pool_id                                                  AS driver_instance_pool_id_cd,
  t1.worker_instance_pool_id                                                  AS worker_instance_pool_id_cd,
  t1.data_security_mode                                                       AS data_security_mode_lb,
  t1.policy_id                                                                AS policy_id_cd,
  t1.tags,
  t1.change_time                                                              AS valid_from_dt,
  LEAD(t1.change_time) OVER (
    PARTITION BY t1.workspace_id, t1.cluster_id
    ORDER BY t1.change_time
  ) - INTERVAL 1 MICROSECOND                                                 AS valid_to_dt,
  CASE
    WHEN LEAD(t1.change_time) OVER (
           PARTITION BY t1.workspace_id, t1.cluster_id ORDER BY t1.change_time
         ) IS NULL THEN 1
    ELSE 0
  END                                                                        AS is_current_fl,
  CASE WHEN t1.delete_time IS NOT NULL THEN 1 ELSE 0 END                      AS is_deleted_fl,
  t1.delete_time                                                              AS delete_dt,
  CURRENT_TIMESTAMP()                                                        AS sys_update_dt
FROM databricks.sal_compute_clusters AS t1
INNER JOIN v_affected_clusters AS t2
  ON  t1.workspace_id = t2.workspace_id_cd
  AND t1.cluster_id   = t2.cluster_id;

-- COMMAND ----------

-- DBTITLE 1,Step 4 — Upsert history table
-- Merge key: (account_id, workspace_id_cd, cluster_id_cd, valid_from_dt) — uniquely identifies a cluster version.
-- UPDATE: fires only when the previously-open record has its window closed
--         (valid_to_dt / is_current_fl changed). All other fields are immutable per version.
-- INSERT: new versions not yet present in silver.
MERGE INTO global_it_hub.td_compute_cluster_history AS tgt
USING v_scd2_source                                  AS src
  ON  tgt.account_id    = src.account_id
  AND tgt.workspace_id_cd  = src.workspace_id_cd
  AND tgt.cluster_id_cd = src.cluster_id_cd
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
  account_id, workspace_id_cd, cluster_id_cd,
  cluster_name_lb, owned_by_cd, create_dt, cluster_source_lb,
  dbr_version_lb, driver_node_type_lb, worker_node_type_lb,
  worker_count_nb, min_autoscale_workers_nb, max_autoscale_workers_nb,
  auto_termination_minutes_nb, enable_elastic_disk_fl,
  driver_instance_pool_id_cd, worker_instance_pool_id_cd,
  data_security_mode_lb, policy_id_cd, tags,
  valid_from_dt, valid_to_dt, is_current_fl, is_deleted_fl, delete_dt,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.cluster_id_cd,
  src.cluster_name_lb, src.owned_by_cd, src.create_dt, src.cluster_source_lb,
  src.dbr_version_lb, src.driver_node_type_lb, src.worker_node_type_lb,
  src.worker_count_nb, src.min_autoscale_workers_nb, src.max_autoscale_workers_nb,
  src.auto_termination_minutes_nb, src.enable_elastic_disk_fl,
  src.driver_instance_pool_id_cd, src.worker_instance_pool_id_cd,
  src.data_security_mode_lb, src.policy_id_cd, src.tags,
  src.valid_from_dt, src.valid_to_dt, src.is_current_fl, src.is_deleted_fl, src.delete_dt,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 5 — Upsert current table
-- Sourced from the history table (single source of truth) rather than re-reading source,
-- so the current table is always consistent with the history table.
-- Scoped to affected clusters only to keep the scan efficient.
-- create_dt and cluster_source_lb are immutable — excluded from UPDATE SET.
MERGE INTO global_it_hub.td_compute_cluster AS tgt
USING (
  SELECT *
  FROM   global_it_hub.td_compute_cluster_history
  WHERE  is_current_fl = 1
    AND  cluster_id_cd IN (SELECT cluster_id FROM v_affected_clusters)
) AS src
  ON  tgt.account_id    = src.account_id
  AND tgt.workspace_id_cd  = src.workspace_id_cd
  AND tgt.cluster_id_cd = src.cluster_id_cd
WHEN MATCHED AND (
     tgt.cluster_name_lb          IS DISTINCT FROM src.cluster_name_lb
  OR tgt.owned_by_cd              IS DISTINCT FROM src.owned_by_cd
  OR tgt.dbr_version_lb           IS DISTINCT FROM src.dbr_version_lb
  OR tgt.driver_node_type_lb      IS DISTINCT FROM src.driver_node_type_lb
  OR tgt.worker_node_type_lb      IS DISTINCT FROM src.worker_node_type_lb
  OR tgt.worker_count_nb          IS DISTINCT FROM src.worker_count_nb
  OR tgt.min_autoscale_workers_nb IS DISTINCT FROM src.min_autoscale_workers_nb
  OR tgt.max_autoscale_workers_nb IS DISTINCT FROM src.max_autoscale_workers_nb
  OR tgt.auto_termination_minutes_nb IS DISTINCT FROM src.auto_termination_minutes_nb
  OR tgt.enable_elastic_disk_fl   IS DISTINCT FROM src.enable_elastic_disk_fl
  OR tgt.driver_instance_pool_id_cd IS DISTINCT FROM src.driver_instance_pool_id_cd
  OR tgt.worker_instance_pool_id_cd IS DISTINCT FROM src.worker_instance_pool_id_cd
  OR tgt.data_security_mode_lb    IS DISTINCT FROM src.data_security_mode_lb
  OR tgt.policy_id_cd             IS DISTINCT FROM src.policy_id_cd
  OR TO_JSON(tgt.tags)            IS DISTINCT FROM TO_JSON(src.tags)
  OR tgt.valid_from_dt            IS DISTINCT FROM src.valid_from_dt
  OR tgt.is_deleted_fl            IS DISTINCT FROM src.is_deleted_fl
) THEN UPDATE SET
  tgt.account_id                  = src.account_id,
  tgt.cluster_name_lb             = src.cluster_name_lb,
  tgt.owned_by_cd                 = src.owned_by_cd,
  tgt.dbr_version_lb              = src.dbr_version_lb,
  tgt.driver_node_type_lb         = src.driver_node_type_lb,
  tgt.worker_node_type_lb         = src.worker_node_type_lb,
  tgt.worker_count_nb             = src.worker_count_nb,
  tgt.min_autoscale_workers_nb    = src.min_autoscale_workers_nb,
  tgt.max_autoscale_workers_nb    = src.max_autoscale_workers_nb,
  tgt.auto_termination_minutes_nb = src.auto_termination_minutes_nb,
  tgt.enable_elastic_disk_fl      = src.enable_elastic_disk_fl,
  tgt.driver_instance_pool_id_cd  = src.driver_instance_pool_id_cd,
  tgt.worker_instance_pool_id_cd  = src.worker_instance_pool_id_cd,
  tgt.data_security_mode_lb       = src.data_security_mode_lb,
  tgt.policy_id_cd                = src.policy_id_cd,
  tgt.tags                        = src.tags,
  tgt.valid_from_dt               = src.valid_from_dt,
  tgt.is_deleted_fl               = src.is_deleted_fl,
  tgt.delete_dt                   = src.delete_dt,
  tgt.sys_update_dt               = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, workspace_id_cd, cluster_id_cd,
  cluster_name_lb, owned_by_cd, create_dt, cluster_source_lb,
  dbr_version_lb, driver_node_type_lb, worker_node_type_lb,
  worker_count_nb, min_autoscale_workers_nb, max_autoscale_workers_nb,
  auto_termination_minutes_nb, enable_elastic_disk_fl,
  driver_instance_pool_id_cd, worker_instance_pool_id_cd,
  data_security_mode_lb, policy_id_cd, tags,
  valid_from_dt, is_deleted_fl, delete_dt,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.cluster_id_cd,
  src.cluster_name_lb, src.owned_by_cd, src.create_dt, src.cluster_source_lb,
  src.dbr_version_lb, src.driver_node_type_lb, src.worker_node_type_lb,
  src.worker_count_nb, src.min_autoscale_workers_nb, src.max_autoscale_workers_nb,
  src.auto_termination_minutes_nb, src.enable_elastic_disk_fl,
  src.driver_instance_pool_id_cd, src.worker_instance_pool_id_cd,
  src.data_security_mode_lb, src.policy_id_cd, src.tags,
  src.valid_from_dt, src.is_deleted_fl, src.delete_dt,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 6 — Advance watermark
-- Written last so a partial failure leaves the watermark unchanged,
-- guaranteeing affected clusters are replayed on the next run.
MERGE INTO global_it_hub._pipeline_watermarks AS tgt
USING (
  SELECT
    'global_it_hub.td_compute_cluster_history'      AS table_name,
    '*'                                              AS account_id,
    COALESCE(
      MAX(t1.sal_load_dt),
      (SELECT last_processed_dt FROM v_watermark)
    )                                               AS last_processed_dt,
    CURRENT_TIMESTAMP()                             AS sys_update_dt
  FROM databricks.sal_compute_clusters AS t1
  INNER JOIN v_affected_clusters AS t2
    ON  t1.workspace_id = t2.workspace_id_cd
    AND t1.cluster_id   = t2.cluster_id
) AS src
  ON  tgt.table_name = src.table_name
  AND tgt.account_id  = src.account_id
WHEN MATCHED THEN UPDATE SET
  tgt.last_processed_dt = src.last_processed_dt,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (table_name, account_id, last_processed_dt, sys_update_dt)
  VALUES (src.table_name, src.account_id, src.last_processed_dt, src.sys_update_dt);
