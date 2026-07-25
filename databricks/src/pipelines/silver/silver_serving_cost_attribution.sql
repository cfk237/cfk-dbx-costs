-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## DLT — Serving Cost Attribution (Materialized View)
-- MAGIC
-- MAGIC Single-MV pipeline. Allocates each endpoint-day's billed cost (`tf_billing_cost`)
-- MAGIC across the AI Gateway requests served by that endpoint that day (`tf_serving_usage`),
-- MAGIC proportionally to token volume.
-- MAGIC
-- MAGIC | Source | Target |
-- MAGIC |---|---|
-- MAGIC | `tf_billing_cost` × `tf_serving_usage` × `td_serving_entity` | `mvf_serving_cost_attribution` (MV) |
-- MAGIC
-- MAGIC **Grain**: one row per serving request (`databricks_request_cd`).
-- MAGIC
-- MAGIC ### Why its own pipeline (not the silver pipeline, not a notebook)
-- MAGIC Same reasoning as `mvf_query_cost_attribution` (`silver_query_cost_attribution.sql`):
-- MAGIC MV DDL is blocked on serverless generic compute
-- MAGIC (`MV_NOT_ENABLED_ON_SERVERLESS_GENERIC_COMPUTE`), and this MV must refresh **after**
-- MAGIC `silver_billing_cost.sql`, which runs after the silver pipeline. Lives in the same
-- MAGIC `attributed_cost` pipeline as `mvf_query_cost_attribution` — both share that ordering
-- MAGIC constraint, so the master job's single `refresh_attributed_cost` task
-- MAGIC (after `run_billing_cost`) refreshes both.
-- MAGIC
-- MAGIC ### Allocation model
-- MAGIC - **Pool**: `SUM(line_cost_usd)` per (usage_dt, workspace, endpoint) from `tf_billing_cost`,
-- MAGIC   filtered to `usage_metadata.endpoint_id IS NOT NULL` — the same billing lines
-- MAGIC   `td_billing_endpoint` is built from.
-- MAGIC - **Weight**: `input_token_count_nb + output_token_count_nb` per request — the natural
-- MAGIC   cost driver for token-billed serving (chat/completions/embeddings). Endpoint-days with
-- MAGIC   zero total tokens (e.g. classic real-time model serving, which isn't token-billed) fall
-- MAGIC   back to an even split by request count.
-- MAGIC - `tf_serving_usage` only carries `served_entity_cd` — joined through `td_serving_entity`
-- MAGIC   to reach the `endpoint_cd` grain the billing cost pool is keyed at. Requests that don't
-- MAGIC   resolve to a known served entity (and therefore no endpoint) are out of scope, same as
-- MAGIC   how `mvf_query_cost_attribution` scopes out statements with no `warehouse_id`.
-- MAGIC - **No date window** — full history; each pipeline update recomputes incrementally where
-- MAGIC   possible. Force a full recompute with the `attributed-cost-full-refresh` job (or a
-- MAGIC   full-refresh trigger in the pipeline UI) — it refreshes every MV in this pipeline.
-- MAGIC
-- MAGIC Attribution is non-incremental at the day level (a late request changes the allocation
-- MAGIC denominator for the whole endpoint-day; a billing restatement changes the pool) — the
-- MAGIC pipeline refresh planner handles that recompute automatically.
-- MAGIC
-- MAGIC This is a proportional **approximation**, not a ground-truth per-request price: token
-- MAGIC count isn't necessarily what Databricks bills on internally for provisioned-throughput
-- MAGIC endpoints, and the request-count fallback is coarser still for non-token serving.

-- COMMAND ----------

-- DBTITLE 1,mvf_serving_cost_attribution
CREATE OR REFRESH MATERIALIZED VIEW mvf_serving_cost_attribution
COMMENT 'Silver fact MV: endpoint cost allocated per AI Gateway serving request, proportional to token volume (falls back to an even split by request count on non-token-billed endpoint-days). Pool = tf_billing_cost per endpoint-day.'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH endpoint_daily_cost AS (
  -- Billed pool to allocate: cost per (day, workspace, endpoint) from the silver cost fact.
  SELECT
    usage_dt,
    account_cd,
    workspace_cd,
    usage_metadata.endpoint_id       AS endpoint_cd,
    SUM(line_cost_usd)               AS endpoint_cost_usd
  FROM global_it_hub.tf_billing_cost
  WHERE usage_metadata.endpoint_id IS NOT NULL
  GROUP BY usage_dt, account_cd, workspace_cd, usage_metadata.endpoint_id
),

serving_base AS (
  -- One row per serving request, resolved to its parent endpoint (tf_serving_usage only
  -- carries served_entity_cd) — the grain the billing cost pool is keyed at.
  SELECT
    t1.databricks_request_cd,
    t1.client_request_cd,
    t1.account_cd,
    t1.workspace_cd,
    t1.region_lb,
    t1.served_entity_cd,
    t2.endpoint_cd,
    t2.endpoint_name_lb,
    t2.entity_type_lb,
    t2.entity_name_lb,
    t1.requester_lb,
    t1.status_code_nb,
    t1.request_dt,
    DATE(t1.request_dt)              AS usage_dt,
    t1.input_token_count_nb,
    t1.output_token_count_nb,
    t1.input_character_count_nb,
    t1.output_character_count_nb,
    t1.request_streaming_fl,
    COALESCE(t1.input_token_count_nb, 0)
      + COALESCE(t1.output_token_count_nb, 0)  AS total_token_count_nb
  FROM global_it_hub.tf_serving_usage AS t1
  LEFT JOIN global_it_hub.td_serving_entity AS t2
    ON  t2.account_cd       = t1.account_cd
    AND t2.workspace_cd     = t1.workspace_cd
    AND t2.served_entity_cd = t1.served_entity_cd
  WHERE t2.endpoint_cd IS NOT NULL
),

daily_endpoint_totals AS (
  -- Denominators of the allocation fraction: total tokens (primary) and total request count
  -- (fallback) per (day, workspace, endpoint).
  SELECT
    usage_dt,
    account_cd,
    workspace_cd,
    endpoint_cd,
    SUM(total_token_count_nb)        AS total_token_count_nb,
    COUNT(*)                         AS total_request_count_nb
  FROM serving_base
  GROUP BY usage_dt, account_cd, workspace_cd, endpoint_cd
)

SELECT
  t1.databricks_request_cd,
  t1.client_request_cd,
  t1.account_cd,
  t1.workspace_cd,
  t1.region_lb,
  t1.served_entity_cd,
  t1.endpoint_cd,
  t1.endpoint_name_lb,
  t1.entity_type_lb,
  t1.entity_name_lb,
  t1.requester_lb,
  t1.status_code_nb,
  t1.request_dt,
  t1.usage_dt,
  t1.input_token_count_nb,
  t1.output_token_count_nb,
  t1.input_character_count_nb,
  t1.output_character_count_nb,
  t1.request_streaming_fl,
  t1.total_token_count_nb,
  t3.endpoint_cost_usd,              -- billed pool for this endpoint-day (validation)
  t2.total_token_count_nb            AS daily_total_token_count_nb,   -- allocation denominator, tokens (validation)
  t2.total_request_count_nb          AS daily_total_request_count_nb, -- allocation denominator, requests (validation)
  ROUND(
    CASE
      WHEN t3.endpoint_cost_usd IS NULL  THEN 0
      WHEN t2.total_token_count_nb > 0   THEN t3.endpoint_cost_usd
                                               * (t1.total_token_count_nb / t2.total_token_count_nb)
      WHEN t2.total_request_count_nb > 0 THEN t3.endpoint_cost_usd
                                               * (1.0 / t2.total_request_count_nb)
      ELSE 0
    END
  , 6)                                AS attributed_cost_usd
FROM serving_base AS t1
INNER JOIN daily_endpoint_totals AS t2
  ON  t2.usage_dt      = t1.usage_dt
  AND t2.account_cd    = t1.account_cd
  AND t2.workspace_cd  = t1.workspace_cd
  AND t2.endpoint_cd   = t1.endpoint_cd
LEFT JOIN endpoint_daily_cost AS t3
  ON  t3.usage_dt      = t1.usage_dt
  AND t3.account_cd    = t1.account_cd
  AND t3.workspace_cd  = t1.workspace_cd
  AND t3.endpoint_cd   = t1.endpoint_cd;
