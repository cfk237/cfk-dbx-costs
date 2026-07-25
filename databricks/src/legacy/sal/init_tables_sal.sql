-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## SAL — Init Staging Tables
-- MAGIC
-- MAGIC One-time (or idempotent) setup notebook. Creates all SAL staging tables.
-- MAGIC Safe to re-run: every statement uses `CREATE TABLE IF NOT EXISTS`.
-- MAGIC
-- MAGIC | Table | Source | Loaded by |
-- MAGIC |---|---|---|
-- MAGIC | `sal_lakeflow_jobs` | `system.lakeflow.jobs` | `sal_ingest_system_tables` |
-- MAGIC | `sal_lakeflow_pipelines` | `system.lakeflow.pipelines` | `sal_ingest_system_tables` |
-- MAGIC | `sal_compute_clusters` | `system.compute.clusters` | `sal_ingest_system_tables` |
-- MAGIC | `sal_compute_warehouses` | `system.compute.warehouses` | `sal_ingest_system_tables` |
-- MAGIC | `sal_billing_usage` | `system.billing.usage` | `sal_ingest_system_tables` |
-- MAGIC | `sal_billing_list_prices` | `system.billing.list_prices` | `sal_ingest_list_prices` |
-- MAGIC | `sal_workspaces` | `system.access.workspaces_latest` | `sal_ingest_workspaces` |
-- MAGIC | `sal_users` | SCIM API users | `sal_ingest_api_identities` |
-- MAGIC | `sal_service_principals` | SCIM API service principals | `sal_ingest_api_identities` |
-- MAGIC
-- MAGIC Run this notebook **once before** the first execution of any SAL ingestion notebook.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog_env", "dev")
-- MAGIC catalog_env = dbutils.widgets.get("catalog_env")
-- MAGIC
-- MAGIC dbutils.widgets.text("catalog_name", "it_security")
-- MAGIC catalog_name = dbutils.widgets.get("catalog_name")
-- MAGIC
-- MAGIC catalog    = f"{catalog_name}_{catalog_env}"
-- MAGIC sal_schema = "databricks"
-- MAGIC
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{sal_schema}")
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC spark.sql(f"USE SCHEMA {sal_schema}")

-- COMMAND ----------

-- DBTITLE 1,Drop all SAL tables (destructive — skip on normal re-runs)
-- Skip this cell on normal re-runs — it is only needed for a full clean re-initialization.
-- Tables are listed in any order (no FK dependencies between SAL tables).
DROP TABLE IF EXISTS databricks.sal_lakeflow_jobs;
DROP TABLE IF EXISTS databricks.sal_lakeflow_pipelines;
DROP TABLE IF EXISTS databricks.sal_compute_clusters;
DROP TABLE IF EXISTS databricks.sal_compute_warehouses;
DROP TABLE IF EXISTS databricks.sal_billing_usage;
DROP TABLE IF EXISTS databricks.sal_billing_list_prices;
DROP TABLE IF EXISTS databricks.sal_workspaces;
DROP TABLE IF EXISTS databricks.sal_users;
DROP TABLE IF EXISTS databricks.sal_service_principals;

-- COMMAND ----------

-- DBTITLE 1,sal_lakeflow_jobs
-- Staging table for system.lakeflow.jobs — one row per (account_id, workspace_id, job_id, change_time).
-- Liquid clustering on account_id for efficient per-account incremental loads.
CREATE TABLE IF NOT EXISTS databricks.sal_lakeflow_jobs (
  account_id         STRING    NOT NULL COMMENT 'Databricks account identifier',
  workspace_id       STRING    NOT NULL COMMENT 'Databricks workspace identifier',
  job_id             STRING    NOT NULL COMMENT 'Unique job identifier',
  name               STRING             COMMENT 'Job display name',
  description        STRING             COMMENT 'Job description',
  creator_user_name  STRING             COMMENT 'Job creator identity',
  create_time        TIMESTAMP          COMMENT 'When the job was first created',
  run_as             STRING             COMMENT 'Identity used for job execution',
  run_as_user_name   STRING             COMMENT 'Display name of run_as identity',
  tags               MAP<STRING, STRING> COMMENT 'Job tags',
  change_time        TIMESTAMP NOT NULL COMMENT 'When this version of the job was recorded',
  delete_time        TIMESTAMP          COMMENT 'Deletion timestamp (NULL if active)',
  -- Staging audit
  sal_load_dt        TIMESTAMP NOT NULL COMMENT 'When this row was loaded into the SAL table'
)
USING DELTA
CLUSTER BY (account_id)
COMMENT 'SAL staging for system.lakeflow.jobs. One row per job version per account.'
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- DBTITLE 1,sal_lakeflow_pipelines
-- Staging table for system.lakeflow.pipelines — one row per (account_id, workspace_id, pipeline_id, change_time).
-- Liquid clustering on account_id for efficient per-account incremental loads.
CREATE TABLE IF NOT EXISTS databricks.sal_lakeflow_pipelines (
  account_id         STRING    NOT NULL COMMENT 'Databricks account identifier',
  workspace_id       STRING    NOT NULL COMMENT 'Databricks workspace identifier',
  pipeline_id        STRING    NOT NULL COMMENT 'Unique pipeline identifier',
  pipeline_type      STRING             COMMENT 'Pipeline category',
  name               STRING             COMMENT 'Pipeline display name',
  created_by         STRING             COMMENT 'Pipeline creator identity',
  create_time        TIMESTAMP          COMMENT 'When the pipeline was first created',
  run_as             STRING             COMMENT 'Identity used for pipeline execution',
  tags               MAP<STRING, STRING> COMMENT 'Pipeline tags',
  configuration      MAP<STRING, STRING> COMMENT 'Pipeline configuration',
  change_time        TIMESTAMP NOT NULL COMMENT 'When this version of the pipeline was recorded',
  delete_time        TIMESTAMP          COMMENT 'Deletion timestamp (NULL if active)',
  -- Staging audit
  sal_load_dt        TIMESTAMP NOT NULL COMMENT 'When this row was loaded into the SAL table'
)
USING DELTA
CLUSTER BY (account_id)
COMMENT 'SAL staging for system.lakeflow.pipelines. One row per pipeline version per account.'
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- DBTITLE 1,sal_compute_clusters
-- Staging table for system.compute.clusters — one row per (account_id, workspace_id, cluster_id, change_time).
-- Liquid clustering on account_id for efficient per-account incremental loads.
-- Cloud-specific structs (aws/azure/gcp_attributes) and init_scripts are excluded.
CREATE TABLE IF NOT EXISTS databricks.sal_compute_clusters (
  account_id                  STRING    NOT NULL COMMENT 'Databricks account identifier',
  workspace_id                STRING    NOT NULL COMMENT 'Databricks workspace identifier',
  cluster_id                  STRING    NOT NULL COMMENT 'Unique cluster identifier',
  cluster_name                STRING             COMMENT 'Cluster display name',
  owned_by                    STRING             COMMENT 'Cluster owner identity',
  create_time                 TIMESTAMP          COMMENT 'When the cluster was first created',
  cluster_source              STRING             COMMENT 'Origin: UI, API, JOB, PIPELINE',
  dbr_version                 STRING             COMMENT 'Databricks Runtime version',
  driver_node_type            STRING             COMMENT 'Driver node instance type',
  worker_node_type            STRING             COMMENT 'Worker node instance type',
  worker_count                BIGINT             COMMENT 'Fixed worker count',
  min_autoscale_workers       BIGINT             COMMENT 'Minimum autoscale workers',
  max_autoscale_workers       BIGINT             COMMENT 'Maximum autoscale workers',
  auto_termination_minutes    BIGINT             COMMENT 'Auto-termination duration in minutes',
  enable_elastic_disk         BOOLEAN            COMMENT 'Autoscaling disk enabled',
  driver_instance_pool_id     STRING             COMMENT 'Driver instance pool ID',
  worker_instance_pool_id     STRING             COMMENT 'Worker instance pool ID',
  data_security_mode          STRING             COMMENT 'Access mode',
  policy_id                   STRING             COMMENT 'Compute policy ID',
  tags                        MAP<STRING, STRING> COMMENT 'Cluster tags',
  change_time                 TIMESTAMP NOT NULL COMMENT 'When this version was recorded',
  delete_time                 TIMESTAMP          COMMENT 'Deletion timestamp (NULL if active)',
  -- Staging audit
  sal_load_dt                 TIMESTAMP NOT NULL COMMENT 'When this row was loaded into the SAL table'
)
USING DELTA
CLUSTER BY (account_id)
COMMENT 'SAL staging for system.compute.clusters. One row per cluster version per account.'
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- DBTITLE 1,sal_compute_warehouses
-- Staging table for system.compute.warehouses — one row per (account_id, workspace_id, warehouse_id, change_time).
-- Liquid clustering on account_id for efficient per-account incremental loads.
-- Note: source table has no create_time column.
CREATE TABLE IF NOT EXISTS databricks.sal_compute_warehouses (
  account_id           STRING    NOT NULL COMMENT 'Databricks account identifier',
  workspace_id         STRING    NOT NULL COMMENT 'Databricks workspace identifier',
  warehouse_id         STRING    NOT NULL COMMENT 'Unique SQL warehouse identifier',
  warehouse_name       STRING             COMMENT 'SQL warehouse display name',
  warehouse_type       STRING             COMMENT 'Warehouse category: CLASSIC, PRO, SERVERLESS',
  warehouse_channel    STRING             COMMENT 'Release channel: CURRENT, PREVIEW',
  warehouse_size       STRING             COMMENT 'Cluster size',
  min_clusters         INT                COMMENT 'Minimum clusters',
  max_clusters         INT                COMMENT 'Maximum clusters',
  auto_stop_minutes    INT                COMMENT 'Idle duration before auto-shutdown',
  tags                 MAP<STRING, STRING> COMMENT 'SQL warehouse tags',
  change_time          TIMESTAMP NOT NULL COMMENT 'When this version was recorded',
  delete_time          TIMESTAMP          COMMENT 'Deletion timestamp (NULL if active)',
  -- Staging audit
  sal_load_dt          TIMESTAMP NOT NULL COMMENT 'When this row was loaded into the SAL table'
)
USING DELTA
CLUSTER BY (account_id)
COMMENT 'SAL staging for system.compute.warehouses. One row per warehouse version per account.'
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- DBTITLE 1,sal_billing_usage
-- Staging table for system.billing.usage — one row per (account_id, record_id).
-- Liquid clustering on (account_id, ingestion_date) for efficient per-account date-range loads.
-- Mirrors tf_billing_usage structure without the computed/enriched columns.
CREATE TABLE IF NOT EXISTS databricks.sal_billing_usage (
  account_id             STRING    NOT NULL COMMENT 'Databricks account identifier',
  workspace_id           STRING    NOT NULL COMMENT 'Databricks workspace identifier',
  record_id              STRING    NOT NULL COMMENT 'Unique billing record identifier',
  sku_name               STRING             COMMENT 'SKU name',
  cloud                  STRING             COMMENT 'Cloud provider',
  usage_start_time       TIMESTAMP          COMMENT 'Start of usage period',
  usage_end_time         TIMESTAMP          COMMENT 'End of usage period',
  usage_date             DATE               COMMENT 'Date when usage occurred',
  custom_tags            MAP<STRING, STRING> COMMENT 'User-defined tags',
  usage_unit             STRING             COMMENT 'Unit of measurement',
  usage_quantity         DOUBLE             COMMENT 'Amount consumed',
  usage_metadata         STRUCT<
                           cluster_id:     STRING,
                           warehouse_id:   STRING,
                           instance_pool_id: STRING,
                           node_type:      STRING,
                           job_id:         STRING,
                           job_run_id:     STRING,
                           notebook_id:    STRING,
                           notebook_path:  STRING,
                           dlt_pipeline_id: STRING,
                           endpoint_id:    STRING,
                           endpoint_name:  STRING,
                           app_id:               STRING,
                           app_name:             STRING,
                           networking_client:    STRING
                         >                  COMMENT 'Usage metadata struct',
  identity_metadata      STRUCT<
                           run_as:         STRING,
                           run_by:         STRING,
                           created_by:     STRING,
                           owned_by:       STRING
                         >                  COMMENT 'Identity metadata struct',
  record_type            STRING             COMMENT 'ORIGINAL, RETRACTION, or RESTATEMENT',
  ingestion_date         DATE      NOT NULL COMMENT 'Date when record was published',
  billing_origin_product STRING             COMMENT 'Billing origin product',
  product_features       STRUCT<
                           jobs_tier:      STRING,
                           is_serverless:  BOOLEAN,
                           is_photon:      BOOLEAN
                         >                  COMMENT 'Product features struct',
  usage_type             STRING             COMMENT 'Usage category',
  -- Staging audit
  sal_load_dt            TIMESTAMP NOT NULL COMMENT 'When this row was loaded into the SAL table'
)
USING DELTA
CLUSTER BY (account_id, ingestion_date)
COMMENT 'SAL staging for system.billing.usage. One row per billing record per account.'
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- DBTITLE 1,sal_workspaces
-- Staging table for system.access.workspaces_latest — one row per workspace in the account.
-- Liquid clustering on account_id for efficient per-account reads.
-- Note: source table does not support streaming; loaded via full-refresh batch extract.
CREATE TABLE IF NOT EXISTS databricks.sal_workspaces (
  account_id     STRING    NOT NULL COMMENT 'Databricks account identifier',
  workspace_id   STRING    NOT NULL COMMENT 'Unique workspace identifier',
  workspace_name STRING             COMMENT 'Workspace display name',
  workspace_url  STRING             COMMENT 'Workspace URL',
  create_time    TIMESTAMP          COMMENT 'When the workspace was created',
  status         STRING             COMMENT 'Workspace status: RUNNING, CANCELLED, etc.',
  -- Staging audit
  sal_load_dt    TIMESTAMP NOT NULL COMMENT 'When this row was loaded into the SAL table'
)
USING DELTA
CLUSTER BY (account_id)
COMMENT 'SAL staging for system.access.workspaces_latest. One row per workspace per load.'
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- DBTITLE 1,sal_billing_list_prices
-- Staging table for system.billing.list_prices — full-refresh on each extract run.
-- Note: source table does not support streaming; loaded via full-refresh batch extract.
CREATE TABLE IF NOT EXISTS databricks.sal_billing_list_prices (
  account_id              STRING    NOT NULL COMMENT 'Databricks account identifier',
  sku_name                STRING             COMMENT 'SKU name',
  cloud                   STRING             COMMENT 'Cloud provider: AWS, AZURE, or GCP',
  currency_code           STRING             COMMENT 'Currency code',
  usage_unit              STRING             COMMENT 'Unit of measurement',
  pricing                 STRUCT<
                            default:        STRING,
                            promotional:    STRUCT<default: STRING>,
                            effective_list: STRUCT<default: STRING>
                          >                  COMMENT 'Pricing struct (default and effective list prices)',
  price_start_time        TIMESTAMP          COMMENT 'When this price became effective',
  price_end_time          TIMESTAMP          COMMENT 'When this price ceased to be effective; NULL if current',
  -- Staging audit
  sal_load_dt             TIMESTAMP NOT NULL COMMENT 'When this row was loaded into the SAL table'
)
USING DELTA
CLUSTER BY (sku_name, cloud)
COMMENT 'SAL staging for system.billing.list_prices. Full snapshot per load run.'
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- DBTITLE 1,sal_users
-- Staging table for Databricks SCIM API users — full-refresh on each extract run.
-- Loaded from Parquet files produced by raw_extract_api_identities.py.
CREATE TABLE IF NOT EXISTS databricks.sal_users (
  user_id_cd       STRING    NOT NULL COMMENT 'Databricks internal user identifier (SCIM id)',
  account_id       STRING             COMMENT 'Databricks account identifier',
  username_lb      STRING             COMMENT 'Username (userName)',
  display_name_lb  STRING             COMMENT 'Display name',
  first_name_lb    STRING             COMMENT 'Given name',
  last_name_lb     STRING             COMMENT 'Family name',
  is_active_fl     INT                COMMENT '1 = active, 0 = inactive',
  external_id_cd   STRING             COMMENT 'External identity provider ID',
  primary_email_lb STRING             COMMENT 'Primary email address',
  sys_load_dt      TIMESTAMP          COMMENT 'When the SCIM API extract ran',
  -- Staging audit
  sal_load_dt      TIMESTAMP NOT NULL COMMENT 'When this row was loaded into the SAL table'
)
USING DELTA
CLUSTER BY (account_id)
COMMENT 'SAL staging for SCIM API users. One row per user per load run.'
TBLPROPERTIES ('quality' = 'bronze');

-- COMMAND ----------

-- DBTITLE 1,sal_service_principals
-- Staging table for Databricks SCIM API service principals — full-refresh on each extract run.
-- Loaded from Parquet files produced by raw_extract_api_identities.py.
CREATE TABLE IF NOT EXISTS databricks.sal_service_principals (
  sp_id_cd          STRING    NOT NULL COMMENT 'Databricks internal service principal identifier (SCIM id)',
  account_id        STRING             COMMENT 'Databricks account identifier',
  application_id_cd STRING             COMMENT 'Azure / OAuth application (client) ID — UUID format',
  display_name_lb   STRING             COMMENT 'Service principal display name',
  is_active_fl      INT                COMMENT '1 = active, 0 = inactive',
  external_id_cd    STRING             COMMENT 'External identity provider ID',
  sys_load_dt       TIMESTAMP          COMMENT 'When the SCIM API extract ran',
  -- Staging audit
  sal_load_dt       TIMESTAMP NOT NULL COMMENT 'When this row was loaded into the SAL table'
)
USING DELTA
CLUSTER BY (account_id)
COMMENT 'SAL staging for SCIM API service principals. One row per SP per load run.'
TBLPROPERTIES ('quality' = 'bronze');
