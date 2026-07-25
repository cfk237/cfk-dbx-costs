-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Billing List Price & Cost (MERGE)
-- MAGIC
-- MAGIC Two MERGE steps in one notebook:
-- MAGIC 1. Upserts `td_billing_list_price` from `billing_list_prices` — a full snapshot
-- MAGIC    refreshed externally (`CREATE OR REPLACE TABLE ... AS SELECT * FROM system.billing.list_prices`),
-- MAGIC    the same pattern as `td_workspace` / `access_workspaces_latest` (see `silver_workspace.sql`):
-- MAGIC    not append-only, so it can't be a DLT streaming source.
-- MAGIC 2. Enriches `tf_billing_usage` with USD cost from the now-current `td_billing_list_price`
-- MAGIC    into the silver fact table `tf_billing_cost`.
-- MAGIC
-- MAGIC | Source | Target |
-- MAGIC |---|---|
-- MAGIC | `billing_list_prices` | `td_billing_list_price` |
-- MAGIC | `tf_billing_usage` × `td_billing_list_price` | `tf_billing_cost` |
-- MAGIC
-- MAGIC **Prerequisites**: run the silver DLT pipeline (`tf_billing_usage`) first.
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC 1. **Upsert `td_billing_list_price`** — full-snapshot `MERGE`, no watermark (every run
-- MAGIC    sees every current price validity window). Merge key:
-- MAGIC    `(account_cd, sku_name_lb, cloud_lb, currency_code_lb, price_start_dt)` — keyed on
-- MAGIC    `price_start_dt` so each validity window is kept as its own row rather than
-- MAGIC    collapsing to one current row per SKU, required by the range join in step 3
-- MAGIC    (`usage_end_dt BETWEEN price_start_dt AND price_end_dt`).
-- MAGIC 2. **Derive watermark** from `tf_billing_cost` itself: `MAX(usage_load_dt)`,
-- MAGIC    where `usage_load_dt` carries the source row's `tf_billing_usage.sys_load_dt`
-- MAGIC 3. **Merge** `tf_billing_usage` (filtered by `sys_load_dt > watermark`) joined with
-- MAGIC    `td_billing_list_price` into `tf_billing_cost`
-- MAGIC
-- MAGIC The `tf_billing_cost` watermark needs no external state: it is read from the target
-- MAGIC itself, so it is self-healing (a RESTORE or rebuild of `tf_billing_cost` automatically
-- MAGIC rewinds it) and arrival-time-correct (backfilled usage rows with old `ingestion_dt`
-- MAGIC still have a fresh `sys_load_dt`, so they are picked up). On an empty target it
-- MAGIC defaults to epoch → full load.
-- MAGIC
-- MAGIC Merge key: `(account_cd, workspace_cd, record_cd, ingestion_dt)` — `ingestion_dt` enables partition pruning on target.
-- MAGIC UPDATE fires when `line_cost_usd` or other tracked fields change (e.g. list price correction).
-- MAGIC RETRACTION and RESTATEMENT records carry their own `record_cd` and are inserted as separate rows.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog_env",  "dev")
-- MAGIC catalog_env  = dbutils.widgets.get("catalog_env")
-- MAGIC
-- MAGIC dbutils.widgets.text("catalog_name", "it_security")
-- MAGIC catalog_name = dbutils.widgets.get("catalog_name")
-- MAGIC
-- MAGIC dbutils.widgets.text("bronze_catalog_name", "sandbox")
-- MAGIC bronze_catalog_name = dbutils.widgets.get("bronze_catalog_name")
-- MAGIC
-- MAGIC dbutils.widgets.text("bronze_schema", "it_security_explo")
-- MAGIC bronze_schema = dbutils.widgets.get("bronze_schema")
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

-- DBTITLE 1,Step 1 — Upsert td_billing_list_price
-- billing_list_prices is refreshed externally as a full snapshot every run — no watermark,
-- straight MERGE. Merge key includes price_start_dt so each price validity window is kept
-- as its own row rather than collapsing to one current row per SKU — required by the range
-- join in Step 3 (usage_end_dt BETWEEN price_start_dt AND price_end_dt).
MERGE INTO global_it_hub.td_billing_list_price AS tgt
USING (
  SELECT
    account_id                                       AS account_cd,
    sku_name                                         AS sku_name_lb,
    cloud                                            AS cloud_lb,
    currency_code                                    AS currency_code_lb,
    usage_unit                                       AS usage_unit_lb,
    CAST(pricing.default AS DOUBLE)                  AS default_price_nb,
    CAST(pricing.effective_list.default AS DOUBLE)   AS effective_list_price_nb,
    price_start_time                                 AS price_start_dt,
    price_end_time                                   AS price_end_dt
  FROM IDENTIFIER(:bronze_catalog_name || '_' || :catalog_env || '.' || :bronze_schema || '.' || 'billing_list_prices')
) AS src
  ON  tgt.account_cd       = src.account_cd
  AND tgt.sku_name_lb      = src.sku_name_lb
  AND tgt.cloud_lb         = src.cloud_lb
  AND tgt.currency_code_lb = src.currency_code_lb
  AND tgt.price_start_dt   = src.price_start_dt
WHEN MATCHED AND (
     tgt.usage_unit_lb           IS DISTINCT FROM src.usage_unit_lb
  OR tgt.default_price_nb        IS DISTINCT FROM src.default_price_nb
  OR tgt.effective_list_price_nb IS DISTINCT FROM src.effective_list_price_nb
  OR tgt.price_end_dt            IS DISTINCT FROM src.price_end_dt
) THEN UPDATE SET
  tgt.usage_unit_lb           = src.usage_unit_lb,
  tgt.default_price_nb        = src.default_price_nb,
  tgt.effective_list_price_nb = src.effective_list_price_nb,
  tgt.price_end_dt            = src.price_end_dt,
  tgt.sys_update_dt           = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  account_cd, sku_name_lb, cloud_lb, currency_code_lb, usage_unit_lb,
  default_price_nb, effective_list_price_nb, price_start_dt, price_end_dt,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_cd, src.sku_name_lb, src.cloud_lb, src.currency_code_lb, src.usage_unit_lb,
  src.default_price_nb, src.effective_list_price_nb, src.price_start_dt, src.price_end_dt,
  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Derive watermark from target
-- Highest tf_billing_usage.sys_load_dt already costed, read from tf_billing_cost itself.
-- Self-healing: a RESTORE or rebuild of tf_billing_cost rewinds the watermark automatically.
-- On first run (empty table, or all-NULL after migration) defaults to epoch → full load;
-- the MERGE is idempotent, so reprocessing is safe.
CREATE OR REPLACE TEMP VIEW v_watermark AS
SELECT COALESCE(
  MAX(usage_load_dt),
  CAST('1970-01-01 00:00:00' AS TIMESTAMP)
) AS last_processed_dt
FROM global_it_hub.tf_billing_cost;

-- COMMAND ----------

-- DBTITLE 1,Step 3 — Upsert tf_billing_cost
-- MERGE WITH SCHEMA EVOLUTION: serverless-compatible alternative to the blocked session config
-- spark.databricks.delta.schema.autoMerge.enabled. Automatically adds new top-level columns
-- and widens nested structs (e.g. usage_metadata) from the source to tf_billing_cost so that
-- new billing resource types propagate without CAST or DDL changes.

-- Source: tf_billing_usage records loaded since the last billing_cost run (sys_load_dt > watermark),
-- joined with the silver list price dimension.
-- Merge key: (record_cd, ingestion_dt) — ingestion_dt enables partition pruning on target.
-- IS DISTINCT FROM guard scoped to line_cost_usd (list price correction) and usage_load_dt
-- (backfills the watermark column on rows that predate it, and re-syncs after a
-- tf_billing_usage full refresh; converges to no-op once equal). INSERT * and UPDATE SET *
-- propagate all columns.
MERGE WITH SCHEMA EVOLUTION INTO global_it_hub.tf_billing_cost AS tgt
USING (
  SELECT
    t1.record_cd,
    t1.account_cd,
    t1.workspace_cd,
    t1.record_type_lb,
    t1.ingestion_dt,
    t1.usage_dt,
    t1.usage_start_dt,
    t1.usage_end_dt,
    t1.product_lb,
    t1.usage_type_lb,
    t1.sku_name_lb,
    t1.cloud_lb,
    t1.usage_unit_lb,
    t1.usage_quantity_nb,
    t1.custom_tags,
    t1.usage_metadata,
    t1.run_as_cd,
    t1.run_by_cd,
    t1.created_by_cd,
    t1.owned_by_cd,
    t1.jobs_tier_lb,
    t1.is_serverless_fl,
    t1.is_photon_fl,
    t1.usage_quantity_nb
      * COALESCE(t2.effective_list_price_nb, t2.default_price_nb)      AS line_cost_usd,
    t1.sys_load_dt                                                       AS usage_load_dt,
    CURRENT_TIMESTAMP()                                                  AS sys_load_dt
  FROM global_it_hub.tf_billing_usage AS t1
  LEFT JOIN global_it_hub.td_billing_list_price AS t2
    ON  t1.account_cd    = t2.account_cd
    AND t1.sku_name_lb   = t2.sku_name_lb
    AND t1.cloud_lb      = t2.cloud_lb
    AND t1.usage_end_dt >= t2.price_start_dt
    AND (t2.price_end_dt IS NULL OR t1.usage_end_dt < t2.price_end_dt)
  WHERE t1.sys_load_dt > (SELECT last_processed_dt FROM v_watermark)
) AS src
  ON  tgt.account_cd      = src.account_cd
  AND tgt.workspace_cd = src.workspace_cd
  AND tgt.record_cd    = src.record_cd
  AND tgt.ingestion_dt  = src.ingestion_dt
WHEN MATCHED AND (
  tgt.line_cost_usd IS DISTINCT FROM src.line_cost_usd
  OR tgt.usage_load_dt IS DISTINCT FROM src.usage_load_dt
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
