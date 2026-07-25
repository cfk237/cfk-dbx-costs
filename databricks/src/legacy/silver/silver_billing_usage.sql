-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Billing Usage & App Dimension
-- MAGIC
-- MAGIC Reads `databricks.sal_billing_usage` **once** into a staging temp table, then feeds two silver targets:
-- MAGIC
-- MAGIC | Target | Type | Purpose |
-- MAGIC |---|---|---|
-- MAGIC | `tf_billing_usage` | Fact | One row per billing record, partitioned by `ingestion_date` |
-- MAGIC | `td_billing_app` | Dimension | App dimension — SCD1 upsert from `usage_metadata.app_id` |
-- MAGIC | `td_billing_notebook` | Dimension | Notebook dimension — SCD1 upsert from `usage_metadata.notebook_id` |
-- MAGIC | `td_billing_endpoint` | Dimension | Serving endpoint dimension — SCD1 upsert from `usage_metadata.endpoint_id` |
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC 1. **Read watermark** from `_pipeline_watermarks`
-- MAGIC 2. **Stage** new SAL records → `v_billing_window` (scanned once; deduped by `record_id`)
-- MAGIC 3. **Merge** `v_billing_window` into `tf_billing_usage` on `(record_id_cd, ingestion_date)`
-- MAGIC 4. **Upsert** `td_billing_app` from `v_billing_window` (app records only, SCD1)
-- MAGIC 5. **Upsert** `td_billing_notebook` from `v_billing_window` (notebook records only, SCD1)
-- MAGIC 6. **Upsert** `td_billing_endpoint` from `v_billing_window` (endpoint records only, SCD1)
-- MAGIC 7. **Advance watermark** to `MAX(sal_load_dt)` of processed records
-- MAGIC
-- MAGIC RESTATEMENT records have their own unique `record_id` and arrive with a current `sal_load_dt`,
-- MAGIC so they are always picked up regardless of how old their `ingestion_date` is — broader coverage
-- MAGIC than a fixed `interval_days` window.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog_env",  "dev")
-- MAGIC catalog_env  = dbutils.widgets.get("catalog_env")
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
CREATE OR REPLACE TEMP VIEW v_watermark AS
SELECT COALESCE(
  MAX(last_processed_dt),
  CAST('1970-01-01 00:00:00' AS TIMESTAMP)
) AS last_processed_dt
FROM global_it_hub._pipeline_watermarks
WHERE table_name = 'global_it_hub.tf_billing_usage'
  AND account_id  = '*';

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Stage new billing records (source scanned once)
-- Filters to SAL records newer than the watermark, then deduplicates by record_id
-- keeping the latest SAL load — Auto Loader appends on each extract run so the same
-- record_id can appear multiple times across runs.
-- All downstream steps read from this temp table.
CREATE OR REPLACE TEMPORARY TABLE v_billing_window AS
SELECT *
FROM databricks.sal_billing_usage
WHERE sal_load_dt > (SELECT last_processed_dt FROM v_watermark)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY record_id
  ORDER BY sal_load_dt DESC
) = 1;

-- COMMAND ----------

-- DBTITLE 1,Step 3 — Upsert tf_billing_usage
-- INSERT * propagates all source columns automatically.
-- Schema auto-merge is enabled via table property (serverless-compatible) instead of SET.
-- Merge key: (record_id_cd, ingestion_date) — ingestion_date enables partition pruning on target.
-- Billing records are immutable: MATCHED path only fires on re-ingestion (idempotency guard).
-- All record_type values (ORIGINAL, RETRACTION, RESTATEMENT) are handled as separate rows.
-- INSERT * propagates all source columns automatically — no column list to maintain.
MERGE INTO global_it_hub.tf_billing_usage AS tgt
USING (
  SELECT
    u.record_id                                                       AS record_id_cd,
    u.account_id,
    u.workspace_id                                                    AS workspace_id_cd,
    u.record_type                                                     AS record_type_lb,
    u.ingestion_date,
    u.usage_date,
    u.usage_start_time                                                AS usage_start_dt,
    u.usage_end_time                                                  AS usage_end_dt,
    u.billing_origin_product                                          AS product_lb,
    u.usage_type                                                      AS usage_type_lb,
    u.sku_name                                                        AS sku_name_lb,
    u.cloud                                                           AS cloud_lb,
    u.usage_unit                                                      AS usage_unit_lb,
    u.usage_quantity                                                  AS usage_quantity_nb,
    u.custom_tags,
    u.usage_metadata,
    COALESCE(
      u.identity_metadata.run_as,
      u.identity_metadata.run_by,
      u.identity_metadata.created_by,
      u.identity_metadata.owned_by
    )                                                                 AS run_as_cd,
    CASE
      WHEN u.identity_metadata.run_as     IS NOT NULL THEN 'run_as'
      WHEN u.identity_metadata.run_by     IS NOT NULL THEN 'run_by'
      WHEN u.identity_metadata.created_by IS NOT NULL THEN 'created_by'
      WHEN u.identity_metadata.owned_by   IS NOT NULL THEN 'owned_by'
      ELSE 'No identity found'
    END                                                               AS identity_type_lb,
    u.product_features.jobs_tier                                      AS jobs_tier_lb,
    CASE WHEN u.product_features.is_serverless THEN 1 ELSE 0 END      AS is_serverless_fl,
    CASE WHEN u.product_features.is_photon     THEN 1 ELSE 0 END      AS is_photon_fl,
    CURRENT_TIMESTAMP()                                               AS sys_load_dt
  FROM v_billing_window AS u
) AS src
  ON  tgt.record_id_cd   = src.record_id_cd
  AND tgt.ingestion_date  = src.ingestion_date
WHEN MATCHED THEN UPDATE SET
  tgt.sys_load_dt = src.sys_load_dt
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- DBTITLE 1,Step 4 — Upsert td_billing_app
-- SCD1 upsert of the app dimension from the staged window.
-- Only rows with a non-null app_id are included — other usage records have no app context.
-- Merge key: (account_id, workspace_id_cd, app_id_cd).
-- ROW_NUMBER() picks the earliest-ingested record per entity — one row guaranteed even on ties.
-- UPDATE fires when any tracked attribute has changed.
MERGE INTO global_it_hub.td_billing_app AS tgt
USING (
  SELECT
    u.account_id,
    u.workspace_id                 AS workspace_id_cd,
    u.usage_metadata.app_id        AS app_id_cd,
    u.billing_origin_product       AS product_lb,
    u.usage_metadata.app_name      AS app_name_lb,
    u.identity_metadata.created_by AS owned_by_cd,
    u.custom_tags                  AS tags,
    CURRENT_TIMESTAMP()            AS sys_update_dt
  FROM v_billing_window AS u
  WHERE u.usage_metadata.app_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY u.account_id, u.workspace_id, u.usage_metadata.app_id
    ORDER BY u.ingestion_date
  ) = 1
) AS src
  ON  tgt.account_id   = src.account_id
  AND tgt.workspace_id_cd = src.workspace_id_cd
  AND tgt.app_id_cd    = src.app_id_cd
WHEN MATCHED AND (
     tgt.product_lb   IS DISTINCT FROM src.product_lb
  OR tgt.app_name_lb  IS DISTINCT FROM src.app_name_lb
  OR tgt.owned_by_cd  IS DISTINCT FROM src.owned_by_cd
  OR TO_JSON(tgt.tags)     IS DISTINCT FROM TO_JSON(src.tags)
) THEN UPDATE SET
  tgt.product_lb    = src.product_lb,
  tgt.app_name_lb   = src.app_name_lb,
  tgt.owned_by_cd   = src.owned_by_cd,
  tgt.tags          = src.tags,
  tgt.sys_update_dt = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, workspace_id_cd, app_id_cd, product_lb, app_name_lb, owned_by_cd, tags,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.app_id_cd, src.product_lb, src.app_name_lb, src.owned_by_cd, src.tags,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 5 — Upsert td_billing_notebook
-- SCD1 upsert of the notebook dimension from the staged window.
-- Only rows with a non-null notebook_id are included.
-- Merge key: (account_id, workspace_id_cd, notebook_id_cd).
-- ROW_NUMBER() picks the earliest-ingested record per entity — one row guaranteed even on ties.
-- UPDATE fires when notebook path or product has changed.
MERGE INTO global_it_hub.td_billing_notebook AS tgt
USING (
  SELECT
    u.account_id,
    u.workspace_id                 AS workspace_id_cd,
    u.usage_metadata.notebook_id   AS notebook_id_cd,
    u.billing_origin_product       AS product_lb,
    u.usage_metadata.notebook_path AS notebook_path_lb,
    u.custom_tags                  AS tags,
    CURRENT_TIMESTAMP()            AS sys_update_dt
  FROM v_billing_window AS u
  WHERE u.usage_metadata.notebook_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY u.account_id, u.workspace_id, u.usage_metadata.notebook_id
    ORDER BY u.ingestion_date
  ) = 1
) AS src
  ON  tgt.account_id     = src.account_id
  AND tgt.workspace_id_cd   = src.workspace_id_cd
  AND tgt.notebook_id_cd = src.notebook_id_cd
WHEN MATCHED AND (
     tgt.product_lb      IS DISTINCT FROM src.product_lb
  OR tgt.notebook_path_lb IS DISTINCT FROM src.notebook_path_lb
  OR TO_JSON(tgt.tags)     IS DISTINCT FROM TO_JSON(src.tags)
) THEN UPDATE SET
  tgt.product_lb       = src.product_lb,
  tgt.notebook_path_lb = src.notebook_path_lb,
  tgt.tags             = src.tags,
  tgt.sys_update_dt    = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, workspace_id_cd, notebook_id_cd, product_lb, notebook_path_lb, tags,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.notebook_id_cd, src.product_lb, src.notebook_path_lb, src.tags,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 6 — Upsert td_billing_endpoint
-- SCD1 upsert of the serving endpoint dimension from the staged window.
-- Only rows with a non-null endpoint_id are included.
-- Merge key: (account_id, workspace_id_cd, endpoint_id_cd).
-- ROW_NUMBER() picks the earliest-ingested record per entity — one row guaranteed even on ties.
-- UPDATE fires when endpoint name or product has changed.
MERGE INTO global_it_hub.td_billing_endpoint AS tgt
USING (
  SELECT
    u.account_id,
    u.workspace_id                 AS workspace_id_cd,
    u.usage_metadata.endpoint_id   AS endpoint_id_cd,
    u.billing_origin_product       AS product_lb,
    u.usage_metadata.endpoint_name AS endpoint_name_lb,
    u.custom_tags                  AS tags,
    CURRENT_TIMESTAMP()            AS sys_update_dt
  FROM v_billing_window AS u
  WHERE u.usage_metadata.endpoint_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY u.account_id, u.workspace_id, u.usage_metadata.endpoint_id
    ORDER BY u.ingestion_date
  ) = 1
) AS src
  ON  tgt.account_id     = src.account_id
  AND tgt.workspace_id_cd   = src.workspace_id_cd
  AND tgt.endpoint_id_cd = src.endpoint_id_cd
WHEN MATCHED AND (
     tgt.product_lb      IS DISTINCT FROM src.product_lb
  OR tgt.endpoint_name_lb IS DISTINCT FROM src.endpoint_name_lb
  OR TO_JSON(tgt.tags)     IS DISTINCT FROM TO_JSON(src.tags)
) THEN UPDATE SET
  tgt.product_lb       = src.product_lb,
  tgt.endpoint_name_lb = src.endpoint_name_lb,
  tgt.tags             = src.tags,
  tgt.sys_update_dt    = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, workspace_id_cd, endpoint_id_cd, product_lb, endpoint_name_lb, tags,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.endpoint_id_cd, src.product_lb, src.endpoint_name_lb, src.tags,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 7 — Upsert td_billing_network
-- SCD1 upsert of the network client dimension from the staged window.
-- Rows included when networking_client IS NOT NULL OR billing_origin_product = 'NETWORKING'.
-- network_client_lb falls back to 'NETWORKING' when networking_client is null (product-level rows).
-- Merge key: (account_id, workspace_id_cd, network_client_lb).
-- ROW_NUMBER() picks the earliest-ingested record per entity — one row guaranteed even on ties.
MERGE INTO global_it_hub.td_billing_network AS tgt
USING (
  SELECT
    u.account_id,
    u.workspace_id                                             AS workspace_id_cd,
    COALESCE(u.usage_metadata.networking_client, 'NETWORKING') AS network_client_lb,
    u.billing_origin_product                                   AS product_lb,
    u.custom_tags                                              AS tags,
    CURRENT_TIMESTAMP()                                        AS sys_update_dt
  FROM v_billing_window AS u
  WHERE u.usage_metadata.networking_client IS NOT NULL
     OR u.billing_origin_product = 'NETWORKING'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY u.account_id, u.workspace_id, COALESCE(u.usage_metadata.networking_client, 'NETWORKING')
    ORDER BY u.ingestion_date
  ) = 1
) AS src
  ON  tgt.account_id        = src.account_id
  AND tgt.workspace_id_cd      = src.workspace_id_cd
  AND tgt.network_client_lb = src.network_client_lb
WHEN MATCHED AND (
     tgt.product_lb IS DISTINCT FROM src.product_lb
  OR TO_JSON(tgt.tags)     IS DISTINCT FROM TO_JSON(src.tags)
) THEN UPDATE SET
  tgt.product_lb    = src.product_lb,
  tgt.tags          = src.tags,
  tgt.sys_update_dt = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, workspace_id_cd, network_client_lb, product_lb, tags,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id_cd, src.network_client_lb, src.product_lb, src.tags,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 8 — Advance watermark
-- Written last so a partial failure leaves the watermark unchanged,
-- guaranteeing affected records are replayed on the next run.
MERGE INTO global_it_hub._pipeline_watermarks AS tgt
USING (
  SELECT
    'global_it_hub.tf_billing_usage'          AS table_name,
    '*'                                        AS account_id,
    COALESCE(
      MAX(sal_load_dt),
      (SELECT last_processed_dt FROM v_watermark)
    )                                          AS last_processed_dt,
    CURRENT_TIMESTAMP()                        AS sys_update_dt
  FROM v_billing_window
) AS src
  ON  tgt.table_name = src.table_name
  AND tgt.account_id  = src.account_id
WHEN MATCHED THEN UPDATE SET
  tgt.last_processed_dt = src.last_processed_dt,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (table_name, account_id, last_processed_dt, sys_update_dt)
  VALUES (src.table_name, src.account_id, src.last_processed_dt, src.sys_update_dt);
