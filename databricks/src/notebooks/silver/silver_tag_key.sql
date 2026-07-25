-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Tag Key Dimension
-- MAGIC
-- MAGIC Builds `td_tag_key` from the keys of the `tags MAP<STRING, STRING>` column
-- MAGIC across eight silver dimension tables.
-- MAGIC
-- MAGIC | Source | tags column |
-- MAGIC |---|---|
-- MAGIC | `td_lakeflow_job` | `tags` |
-- MAGIC | `td_compute_warehouse` | `tags` |
-- MAGIC | `td_lakeflow_pipeline` | `tags` |
-- MAGIC | `td_compute_cluster` | `tags` |
-- MAGIC | `td_billing_app` | `tags` |
-- MAGIC | `td_billing_notebook` | `tags` |
-- MAGIC | `td_billing_endpoint` | `tags` |
-- MAGIC | `td_billing_network` | `tags` |
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC - `map_keys(tags)` extracts the key array from each MAP; `LATERAL VIEW EXPLODE` turns keys into rows
-- MAGIC - All eight sources are combined via `UNION ALL` then deduplicated on `tag_key_cd`
-- MAGIC - Natural key: `tag_key_cd` only — tag key names are global (workspace-agnostic)
-- MAGIC - `WHEN NOT MATCHED` only — tag key names are immutable by definition; no UPDATE needed
-- MAGIC
-- MAGIC **Prerequisites**: run the SCD2 silver load notebooks (jobs, pipelines, clusters, warehouses)
-- MAGIC and the billing usage load (`silver_billing_usage` / DLT `silver_billing`) before this
-- MAGIC notebook so the source dimension tables are populated.

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
-- MAGIC
-- MAGIC print(f"Catalog : {catalog}")
-- MAGIC print(f"Schema  : {silver_schema}")

-- COMMAND ----------

-- DBTITLE 1,Tag key normalization mapping
-- Lookup table keyed on UPPER(raw_key) — add new rows here to extend normalization rules.
-- Matching is case-insensitive: 'app_id', 'APP_ID', 'App_Id' all resolve to 'AppId'.
CREATE OR REPLACE TEMPORARY TABLE v_tag_key_mapping AS
SELECT UPPER(raw_pattern) AS raw_pattern_upper, normalized_lb
FROM (
  VALUES
    ('APP_ID',            'AppId'),
    ('DATA_PRODUCT',      'Data Product'),
    ('DATAPRODUCT',       'Data Product'),
    ('DATAPRODUCT_TYPE',  'Data Product Type'),
    ('DATA_PRODUCT_TYPE', 'Data Product Type'),
    ('ENV',               'Environment')
) AS t(raw_pattern, normalized_lb);

-- COMMAND ----------

-- DBTITLE 1,Upsert td_tag_key
-- Natural key: tag_key_cd — one row per distinct tag key across all sources.
-- No UPDATE clause: a tag key name is immutable; only new keys trigger an INSERT.
-- tag_key_id is auto-assigned by Delta IDENTITY on INSERT — excluded from INSERT column list.
-- Normalization is applied via LEFT JOIN with v_tag_key_mapping; unmapped keys keep their
-- trimmed raw value. QUALIFY deduplicates on UPPER(normalised) to handle case variants.
MERGE INTO global_it_hub.td_tag_key AS tgt
USING (
  WITH raw_tag_keys AS (

    -- Jobs — TRIM to strip accidental whitespace from map keys
    SELECT TRIM(tag_key_cd) AS tag_key_cd
    FROM global_it_hub.td_lakeflow_job AS j
    LATERAL VIEW EXPLODE(map_keys(j.tags)) exploded AS tag_key_cd
    WHERE j.job_id > 0 AND j.tags IS NOT NULL

    UNION ALL

    -- Warehouses
    SELECT TRIM(tag_key_cd) AS tag_key_cd
    FROM global_it_hub.td_compute_warehouse AS w
    LATERAL VIEW EXPLODE(map_keys(w.tags)) exploded AS tag_key_cd
    WHERE w.warehouse_id > 0 AND w.tags IS NOT NULL

    UNION ALL

    -- Pipelines
    SELECT TRIM(tag_key_cd) AS tag_key_cd
    FROM global_it_hub.td_lakeflow_pipeline AS p
    LATERAL VIEW EXPLODE(map_keys(p.tags)) exploded AS tag_key_cd
    WHERE p.pipeline_id > 0 AND p.tags IS NOT NULL

    UNION ALL

    -- Clusters
    SELECT TRIM(tag_key_cd) AS tag_key_cd
    FROM global_it_hub.td_compute_cluster AS c
    LATERAL VIEW EXPLODE(map_keys(c.tags)) exploded AS tag_key_cd
    WHERE c.cluster_id > 0 AND c.tags IS NOT NULL

    UNION ALL

    -- Billing — Apps
    SELECT TRIM(tag_key_cd) AS tag_key_cd
    FROM global_it_hub.td_billing_app AS ba
    LATERAL VIEW EXPLODE(map_keys(ba.tags)) exploded AS tag_key_cd
    WHERE ba.app_id > 0 AND ba.tags IS NOT NULL

    UNION ALL

    -- Billing — Notebooks
    SELECT TRIM(tag_key_cd) AS tag_key_cd
    FROM global_it_hub.td_billing_notebook AS bn
    LATERAL VIEW EXPLODE(map_keys(bn.tags)) exploded AS tag_key_cd
    WHERE bn.notebook_id > 0 AND bn.tags IS NOT NULL

    UNION ALL

    -- Billing — Endpoints
    SELECT TRIM(tag_key_cd) AS tag_key_cd
    FROM global_it_hub.td_billing_endpoint AS be
    LATERAL VIEW EXPLODE(map_keys(be.tags)) exploded AS tag_key_cd
    WHERE be.endpoint_id > 0 AND be.tags IS NOT NULL

    UNION ALL

    -- Billing — Networks
    SELECT TRIM(tag_key_cd) AS tag_key_cd
    FROM global_it_hub.td_billing_network AS bw
    LATERAL VIEW EXPLODE(map_keys(bw.tags)) exploded AS tag_key_cd
    WHERE bw.network_id > 0 AND bw.tags IS NOT NULL

  )
  SELECT
    COALESCE(m.normalized_lb, t.tag_key_cd) AS tag_key_cd,
    t.tag_key_cd                             AS raw_key_lb,
    CURRENT_TIMESTAMP()                      AS sys_update_dt
  FROM raw_tag_keys AS t
  LEFT JOIN v_tag_key_mapping AS m
    ON UPPER(t.tag_key_cd) = m.raw_pattern_upper
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY UPPER(COALESCE(m.normalized_lb, t.tag_key_cd))
    ORDER BY t.tag_key_cd
  ) = 1
) AS src ON tgt.tag_key_cd = src.tag_key_cd
WHEN NOT MATCHED THEN INSERT (tag_key_cd, raw_key_lb, sys_create_dt, sys_update_dt)
VALUES (src.tag_key_cd, src.raw_key_lb, src.sys_update_dt, src.sys_update_dt);
