-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Billing List Prices (SCD1 with source validity windows)
-- MAGIC
-- MAGIC Loads `databricks.sal_billing_list_prices` into `global_it_hub.td_billing_list_price`.
-- MAGIC
-- MAGIC | Source | Target |
-- MAGIC |---|---|
-- MAGIC | `databricks.sal_billing_list_prices` | `global_it_hub.td_billing_list_price` |
-- MAGIC
-- MAGIC The source already carries validity windows (`price_start_time` / `price_end_time`),
-- MAGIC so no SCD2 computation is required — the MERGE key includes `price_start_dt`.
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC 1. Read the high-water-mark from `_pipeline_watermarks`
-- MAGIC 2. Identify price records with any new SAL load since the last run; deduplicate to the
-- MAGIC    latest snapshot per `(account_id, sku_name, cloud, currency_code, price_start_time)`
-- MAGIC 3. `MERGE` into `td_billing_list_price` — UPDATE when validity or pricing values change,
-- MAGIC    INSERT new records
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
WHERE table_name = 'global_it_hub.td_billing_list_price'
  AND account_id  = '*';

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Identify affected price records
-- Filters to SAL records newer than the watermark, then deduplicates to the latest
-- snapshot per natural key — SAL accumulates full snapshots on each extract run.
CREATE OR REPLACE TEMPORARY TABLE v_affected_prices AS
SELECT
  account_id,
  sku_name                                               AS sku_name_lb,
  cloud                                                  AS cloud_lb,
  currency_code                                          AS currency_code_lb,
  usage_unit                                             AS usage_unit_lb,
  CAST(pricing.default AS DOUBLE)                        AS default_price_nb,
  CAST(pricing.effective_list.default AS DOUBLE)         AS effective_list_price_nb,
  price_start_time                                       AS price_start_dt,
  price_end_time                                         AS price_end_dt,
  sal_load_dt,
  CURRENT_TIMESTAMP()                                    AS sys_update_dt
FROM databricks.sal_billing_list_prices
WHERE sal_load_dt > (SELECT last_processed_dt FROM v_watermark)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY account_id, sku_name, cloud, currency_code, price_start_time
  ORDER BY sal_load_dt DESC
) = 1;

-- COMMAND ----------

-- DBTITLE 1,Step 3 — Upsert silver dimension
-- Merge key: (account_id, sku_name_lb, cloud_lb, currency_code_lb, price_start_dt).
-- UPDATE: fires when price_end_dt is populated (price closed) or pricing values change.
-- INSERT: new price records not yet in silver.
MERGE INTO global_it_hub.td_billing_list_price AS tgt
USING v_affected_prices                         AS src
  ON  tgt.account_id       = src.account_id
  AND tgt.sku_name_lb      = src.sku_name_lb
  AND tgt.cloud_lb         = src.cloud_lb
  AND tgt.currency_code_lb = src.currency_code_lb
  AND tgt.price_start_dt   = src.price_start_dt
WHEN MATCHED AND (
     tgt.price_end_dt            IS DISTINCT FROM src.price_end_dt
  OR tgt.default_price_nb        IS DISTINCT FROM src.default_price_nb
  OR tgt.effective_list_price_nb IS DISTINCT FROM src.effective_list_price_nb
  OR tgt.usage_unit_lb           IS DISTINCT FROM src.usage_unit_lb
) THEN UPDATE SET
  tgt.price_end_dt            = src.price_end_dt,
  tgt.default_price_nb        = src.default_price_nb,
  tgt.effective_list_price_nb = src.effective_list_price_nb,
  tgt.usage_unit_lb           = src.usage_unit_lb,
  tgt.sys_update_dt           = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, sku_name_lb, cloud_lb, currency_code_lb, usage_unit_lb,
  default_price_nb, effective_list_price_nb,
  price_start_dt, price_end_dt,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.sku_name_lb, src.cloud_lb, src.currency_code_lb, src.usage_unit_lb,
  src.default_price_nb, src.effective_list_price_nb,
  src.price_start_dt, src.price_end_dt,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 4 — Advance watermark
MERGE INTO global_it_hub._pipeline_watermarks AS tgt
USING (
  SELECT
    'global_it_hub.td_billing_list_price'  AS table_name,
    '*'                                    AS account_id,
    COALESCE(
      MAX(sal_load_dt),
      (SELECT last_processed_dt FROM v_watermark)
    )                                      AS last_processed_dt,
    CURRENT_TIMESTAMP()                    AS sys_update_dt
  FROM v_affected_prices
) AS src
  ON  tgt.table_name = src.table_name
  AND tgt.account_id  = src.account_id
WHEN MATCHED THEN UPDATE SET
  tgt.last_processed_dt = src.last_processed_dt,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (table_name, account_id, last_processed_dt, sys_update_dt)
  VALUES (src.table_name, src.account_id, src.last_processed_dt, src.sys_update_dt);
