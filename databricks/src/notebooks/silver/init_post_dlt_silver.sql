-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## DLT Silver — Post-Pipeline Initialisation
-- MAGIC
-- MAGIC **Run once after the first DLT pipeline execution** to insert sentinel rows into the
-- MAGIC DLT-managed silver tables. After the first run this notebook is idempotent — every
-- MAGIC MERGE uses `WHEN NOT MATCHED` so re-running is safe.
-- MAGIC
-- MAGIC ### Why this notebook exists
-- MAGIC DLT creates and owns the lifecycle of its managed streaming tables. Pre-creating them
-- MAGIC with `legacy/silver/init_tables_silver_dlt_managed.sql` causes a conflict (`MANAGED table
-- MAGIC already exists`). Instead:
-- MAGIC 1. Run the DLT pipeline → tables are created by DLT
-- MAGIC 2. Run **this notebook** → sentinel rows and identity column adjustments are applied
-- MAGIC
-- MAGIC ### Tables initialised
-- MAGIC | Table | Sentinels |
-- MAGIC |---|---|
-- MAGIC | `td_lakeflow_job` | -2 Unassigned, -1 Unknown |
-- MAGIC | `td_lakeflow_pipeline` | -2 Unassigned, -1 Unknown |
-- MAGIC | `td_compute_cluster` | -2 Unassigned, -1 Unknown |
-- MAGIC | `td_compute_warehouse` | -2 Unassigned, -1 Unknown |
-- MAGIC | `td_billing_app` | -2 Unassigned, -1 Unknown |
-- MAGIC | `td_billing_notebook` | -2 Unassigned, -1 Unknown |
-- MAGIC | `td_billing_endpoint` | -2 Unassigned, -1 Unknown |
-- MAGIC | `td_billing_network` | -2 Unassigned, -1 Unknown |
-- MAGIC | `td_billing_data_quality_monitoring` | -2 Unassigned, -1 Unknown |
-- MAGIC | `td_serving_entity` | -2 Unassigned, -1 Unknown |

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

-- DBTITLE 1,td_lakeflow_job — sentinel rows
-- Note: column is 'change_time' (from source view), not 'valid_from_dt' (original DDL)
MERGE INTO global_it_hub.td_lakeflow_job AS tgt
USING (
  VALUES
    (-2, 'UNS', 'UNS', 'UNS', 'Unassigned', 'Unassigned', 'UNS', CURRENT_TIMESTAMP(), 'UNS', 'Unassigned', NULL, CURRENT_TIMESTAMP(), 0, NULL, CURRENT_TIMESTAMP()),
    (-1, 'UNK', 'UNK', 'UNK', 'Unknown',    'Unknown',    'UNK', CURRENT_TIMESTAMP(), 'UNK', 'Unknown',    NULL, CURRENT_TIMESTAMP(), 0, NULL, CURRENT_TIMESTAMP())
  AS src(job_id, account_cd, workspace_cd, job_cd, job_name_lb, description_lb, creator_userid_cd, create_dt, run_as_cd, run_as_user_lb, tags, change_time, is_deleted_fl, delete_dt, sys_load_dt)
) ON tgt.job_id = src.job_id
WHEN NOT MATCHED THEN INSERT (job_id, account_cd, workspace_cd, job_cd, job_name_lb, description_lb, creator_userid_cd, create_dt, run_as_cd, run_as_user_lb, tags, change_time, is_deleted_fl, delete_dt, sys_load_dt)
VALUES (src.job_id, src.account_cd, src.workspace_cd, src.job_cd, src.job_name_lb, src.description_lb, src.creator_userid_cd, src.create_dt, src.run_as_cd, src.run_as_user_lb, src.tags, src.change_time, src.is_deleted_fl, src.delete_dt, src.sys_load_dt);

-- COMMAND ----------

-- DBTITLE 1,td_lakeflow_pipeline — sentinel rows
MERGE INTO global_it_hub.td_lakeflow_pipeline AS tgt
USING (
  VALUES
    (-2, 'UNS', 'UNS', 'UNS', 'UNS', 'Unassigned', 'UNS', CURRENT_TIMESTAMP(), 'UNS', NULL, NULL, CURRENT_TIMESTAMP(), 0, NULL, CURRENT_TIMESTAMP()),
    (-1, 'UNK', 'UNK', 'UNK', 'UNK', 'Unknown',    'UNK', CURRENT_TIMESTAMP(), 'UNK', NULL, NULL, CURRENT_TIMESTAMP(), 0, NULL, CURRENT_TIMESTAMP())
  AS src(pipeline_id, account_cd, workspace_cd, pipeline_cd, pipeline_type_lb, pipeline_name_lb, created_by_cd, create_dt, run_as_cd, tags, configuration, change_time, is_deleted_fl, delete_dt, sys_load_dt)
) ON tgt.pipeline_id = src.pipeline_id
WHEN NOT MATCHED THEN INSERT (pipeline_id, account_cd, workspace_cd, pipeline_cd, pipeline_type_lb, pipeline_name_lb, created_by_cd, create_dt, run_as_cd, tags, configuration, change_time, is_deleted_fl, delete_dt, sys_load_dt)
VALUES (src.pipeline_id, src.account_cd, src.workspace_cd, src.pipeline_cd, src.pipeline_type_lb, src.pipeline_name_lb, src.created_by_cd, src.create_dt, src.run_as_cd, src.tags, src.configuration, src.change_time, src.is_deleted_fl, src.delete_dt, src.sys_load_dt);

-- COMMAND ----------

-- DBTITLE 1,td_compute_cluster — sentinel rows
MERGE INTO global_it_hub.td_compute_cluster AS tgt
USING (
  VALUES
    (-2, 'UNS', 'UNS', 'UNS', 'Unassigned', 'UNS', CURRENT_TIMESTAMP(), 'UNS', 'Unassigned', 'Unassigned', 'Unassigned', NULL, NULL, NULL, NULL, 0, NULL, NULL, 'UNS', 'UNS', NULL, CURRENT_TIMESTAMP(), 0, NULL, CURRENT_TIMESTAMP()),
    (-1, 'UNK', 'UNK', 'UNK', 'Unknown',    'UNK', CURRENT_TIMESTAMP(), 'UNK', 'Unknown',    'Unknown',    'Unknown',    NULL, NULL, NULL, NULL, 0, NULL, NULL, 'UNK', 'UNK', NULL, CURRENT_TIMESTAMP(), 0, NULL, CURRENT_TIMESTAMP())
  AS src(cluster_id, account_cd, workspace_cd, cluster_cd, cluster_name_lb, owned_by_cd, create_dt, cluster_source_lb, dbr_version_lb, driver_node_type_lb, worker_node_type_lb, worker_count_nb, min_autoscale_workers_nb, max_autoscale_workers_nb, auto_termination_minutes_nb, enable_elastic_disk_fl, driver_instance_pool_cd, worker_instance_pool_cd, data_security_mode_lb, policy_cd, tags, change_time, is_deleted_fl, delete_dt, sys_load_dt)
) ON tgt.cluster_id = src.cluster_id
WHEN NOT MATCHED THEN INSERT (cluster_id, account_cd, workspace_cd, cluster_cd, cluster_name_lb, owned_by_cd, create_dt, cluster_source_lb, dbr_version_lb, driver_node_type_lb, worker_node_type_lb, worker_count_nb, min_autoscale_workers_nb, max_autoscale_workers_nb, auto_termination_minutes_nb, enable_elastic_disk_fl, driver_instance_pool_cd, worker_instance_pool_cd, data_security_mode_lb, policy_cd, tags, change_time, is_deleted_fl, delete_dt, sys_load_dt)
VALUES (src.cluster_id, src.account_cd, src.workspace_cd, src.cluster_cd, src.cluster_name_lb, src.owned_by_cd, src.create_dt, src.cluster_source_lb, src.dbr_version_lb, src.driver_node_type_lb, src.worker_node_type_lb, src.worker_count_nb, src.min_autoscale_workers_nb, src.max_autoscale_workers_nb, src.auto_termination_minutes_nb, src.enable_elastic_disk_fl, src.driver_instance_pool_cd, src.worker_instance_pool_cd, src.data_security_mode_lb, src.policy_cd, src.tags, src.change_time, src.is_deleted_fl, src.delete_dt, src.sys_load_dt);

-- COMMAND ----------

-- DBTITLE 1,td_compute_warehouse — sentinel rows
MERGE INTO global_it_hub.td_compute_warehouse AS tgt
USING (
  VALUES
    (-2, 'UNS', 'UNS', 'UNS', 'Unassigned', 'UNS', 'UNS', 'UNS', NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP(), 0, NULL, CURRENT_TIMESTAMP()),
    (-1, 'UNK', 'UNK', 'UNK', 'Unknown',    'UNK', 'UNK', 'UNK', NULL, NULL, NULL, NULL, CURRENT_TIMESTAMP(), 0, NULL, CURRENT_TIMESTAMP())
  AS src(warehouse_id, account_cd, workspace_cd, warehouse_cd, warehouse_name_lb, warehouse_type_lb, warehouse_channel_lb, warehouse_size_lb, min_clusters_nb, max_clusters_nb, auto_stop_minutes_nb, tags, change_time, is_deleted_fl, delete_dt, sys_load_dt)
) ON tgt.warehouse_id = src.warehouse_id
WHEN NOT MATCHED THEN INSERT (warehouse_id, account_cd, workspace_cd, warehouse_cd, warehouse_name_lb, warehouse_type_lb, warehouse_channel_lb, warehouse_size_lb, min_clusters_nb, max_clusters_nb, auto_stop_minutes_nb, tags, change_time, is_deleted_fl, delete_dt, sys_load_dt)
VALUES (src.warehouse_id, src.account_cd, src.workspace_cd, src.warehouse_cd, src.warehouse_name_lb, src.warehouse_type_lb, src.warehouse_channel_lb, src.warehouse_size_lb, src.min_clusters_nb, src.max_clusters_nb, src.auto_stop_minutes_nb, src.tags, src.change_time, src.is_deleted_fl, src.delete_dt, src.sys_load_dt);

-- COMMAND ----------

-- DBTITLE 1,td_billing_app — sentinel rows
MERGE INTO global_it_hub.td_billing_app AS tgt
USING (
  VALUES
    (-2, 'UNS', 'UNS', 'UNS', 'UNS', 'Unassigned', 'UNS', NULL),
    (-1, 'UNK', 'UNK', 'UNK', 'UNK', 'Unknown',    'UNK', NULL)
  AS src(app_id, account_cd, workspace_cd, app_cd, product_lb, app_name_lb, run_as_cd, tags)
) ON tgt.app_id = src.app_id
WHEN NOT MATCHED THEN INSERT (app_id, account_cd, workspace_cd, app_cd, product_lb, app_name_lb, run_as_cd, tags, sys_load_dt)
VALUES (src.app_id, src.account_cd, src.workspace_cd, src.app_cd, src.product_lb, src.app_name_lb, src.run_as_cd, src.tags, CURRENT_TIMESTAMP());

-- COMMAND ----------

-- DBTITLE 1,td_billing_notebook — sentinel rows
MERGE INTO global_it_hub.td_billing_notebook AS tgt
USING (
  VALUES
    (-2, 'UNS', 'UNS', 'UNS', 'UNS', 'Unassigned', NULL),
    (-1, 'UNK', 'UNK', 'UNK', 'UNK', 'Unknown',    NULL)
  AS src(notebook_id, account_cd, workspace_cd, notebook_cd, product_lb, notebook_path_lb, tags)
) ON tgt.notebook_id = src.notebook_id
WHEN NOT MATCHED THEN INSERT (notebook_id, account_cd, workspace_cd, notebook_cd, product_lb, notebook_path_lb, tags, sys_load_dt)
VALUES (src.notebook_id, src.account_cd, src.workspace_cd, src.notebook_cd, src.product_lb, src.notebook_path_lb, src.tags, CURRENT_TIMESTAMP());

-- COMMAND ----------

-- DBTITLE 1,td_billing_endpoint — sentinel rows
MERGE INTO global_it_hub.td_billing_endpoint AS tgt
USING (
  VALUES
    (-2, 'UNS', 'UNS', 'UNS', 'UNS', 'Unassigned', NULL),
    (-1, 'UNK', 'UNK', 'UNK', 'UNK', 'Unknown',    NULL)
  AS src(endpoint_id, account_cd, workspace_cd, endpoint_cd, product_lb, endpoint_name_lb, tags)
) ON tgt.endpoint_id = src.endpoint_id
WHEN NOT MATCHED THEN INSERT (endpoint_id, account_cd, workspace_cd, endpoint_cd, product_lb, endpoint_name_lb, tags, sys_load_dt)
VALUES (src.endpoint_id, src.account_cd, src.workspace_cd, src.endpoint_cd, src.product_lb, src.endpoint_name_lb, src.tags, CURRENT_TIMESTAMP());

-- COMMAND ----------

-- DBTITLE 1,td_billing_network — sentinel rows
MERGE INTO global_it_hub.td_billing_network AS tgt
USING (
  VALUES
    (-2, 'UNS', 'UNS', 'UNS', 'Unassigned', NULL),
    (-1, 'UNK', 'UNK', 'UNK', 'Unknown',    NULL)
  AS src(network_id, account_cd, workspace_cd, network_client_cd, product_lb, tags)
) ON tgt.network_id = src.network_id
WHEN NOT MATCHED THEN INSERT (network_id, account_cd, workspace_cd, network_client_cd, product_lb, tags, sys_load_dt)
VALUES (src.network_id, src.account_cd, src.workspace_cd, src.network_client_cd, src.product_lb, src.tags, CURRENT_TIMESTAMP());

-- COMMAND ----------

-- DBTITLE 1,td_billing_data_quality_monitoring — sentinel rows
MERGE INTO global_it_hub.td_billing_data_quality_monitoring AS tgt
USING (
  VALUES
    (-2, 'UNS', 'UNS', 'UNS', 'UNS', 'UNS', NULL),
    (-1, 'UNK', 'UNK', 'UNK', 'UNK', 'UNK', NULL)
  AS src(dqm_id, account_cd, workspace_cd, dqm_cd, product_lb, created_by_cd, tags)
) ON tgt.dqm_id = src.dqm_id
WHEN NOT MATCHED THEN INSERT (dqm_id, account_cd, workspace_cd, dqm_cd, product_lb, created_by_cd, tags, sys_load_dt)
VALUES (src.dqm_id, src.account_cd, src.workspace_cd, src.dqm_cd, src.product_lb, src.created_by_cd, src.tags, CURRENT_TIMESTAMP());

-- COMMAND ----------

-- DBTITLE 1,td_serving_entity — sentinel rows
MERGE INTO global_it_hub.td_serving_entity AS tgt
USING (
  VALUES
    (-2, 'UNS', 'UNS', 'UNS', 'UNS', 'Unassigned', 'Unassigned', 'UNS', 0, CURRENT_TIMESTAMP(), NULL, 0),
    (-1, 'UNK', 'UNK', 'UNK', 'UNK', 'Unknown',    'Unknown',    'UNK', 0, CURRENT_TIMESTAMP(), NULL, 0)
  AS src(entity_id, account_cd, workspace_cd, served_entity_cd, endpoint_cd, endpoint_name_lb, served_entity_name_lb, entity_type_lb, endpoint_config_version_nb, change_time, endpoint_delete_dt, is_deleted_fl)
) ON tgt.entity_id = src.entity_id
WHEN NOT MATCHED THEN INSERT (entity_id, account_cd, workspace_cd, served_entity_cd, endpoint_cd, endpoint_name_lb, served_entity_name_lb, entity_type_lb, endpoint_config_version_nb, change_time, endpoint_delete_dt, is_deleted_fl, sys_load_dt)
VALUES (src.entity_id, src.account_cd, src.workspace_cd, src.served_entity_cd, src.endpoint_cd, src.endpoint_name_lb, src.served_entity_name_lb, src.entity_type_lb, src.endpoint_config_version_nb, src.change_time, src.endpoint_delete_dt, src.is_deleted_fl, CURRENT_TIMESTAMP());
