-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## DLT — Query Cost Attribution (Materialized View)
-- MAGIC
-- MAGIC Single-MV pipeline. Allocates each warehouse-day's billed cost (`tf_billing_cost`)
-- MAGIC across the statements that ran on that warehouse that day (`tf_query_history`),
-- MAGIC proportionally to billable duration.
-- MAGIC
-- MAGIC | Source | Target |
-- MAGIC |---|---|
-- MAGIC | `tf_billing_cost` × `tf_query_history` | `mvf_query_cost_attribution` (MV) |
-- MAGIC
-- MAGIC **Grain**: one row per statement execution (`statement_cd`).
-- MAGIC
-- MAGIC ### Why its own pipeline (not the silver pipeline, not a notebook)
-- MAGIC - MV DDL is blocked on serverless generic compute (`MV_NOT_ENABLED_ON_SERVERLESS_GENERIC_COMPUTE`)
-- MAGIC   — MVs must be owned by a pipeline or created from a SQL warehouse.
-- MAGIC - It must refresh **after** `silver_billing_cost.sql`; the silver pipeline runs before it,
-- MAGIC   so the MV cannot live there. The master job triggers this pipeline after `run_billing_cost`.
-- MAGIC
-- MAGIC ### Allocation model
-- MAGIC - **Pool**: `SUM(line_cost_usd)` per (usage_dt, workspace, warehouse) from `tf_billing_cost`
-- MAGIC   — same cost figures as the rest of the report, price corrections included.
-- MAGIC - **Weight**: billable duration = `total_duration_ms − waiting_at_capacity_ms`, floored at 1 ms.
-- MAGIC   Cold-start wait included (the cluster is billing); queue wait excluded (that cost belongs
-- MAGIC   to the queries already holding capacity). Result-cache hits get weight 0.
-- MAGIC - **All execution statuses kept** (FAILED/CANCELED consumed compute) — slice in Power BI.
-- MAGIC - **No date window** — full history; each pipeline update recomputes incrementally where
-- MAGIC   possible. Force a full recompute with the `query-cost-full-refresh` job (or a
-- MAGIC   full-refresh trigger in the pipeline UI).
-- MAGIC
-- MAGIC Attribution is non-incremental at the day level (a late statement changes the allocation
-- MAGIC denominator for the whole warehouse-day; a billing restatement changes the pool) — the
-- MAGIC pipeline refresh planner handles that recompute automatically.

-- COMMAND ----------

-- DBTITLE 1,mvf_query_cost_attribution
CREATE OR REFRESH MATERIALIZED VIEW mvf_query_cost_attribution
COMMENT 'Silver fact MV: warehouse cost allocated per statement execution, proportional to billable duration. Pool = tf_billing_cost per warehouse-day; weight = total_duration_ms minus queue wait; cache hits weigh 0.'
TBLPROPERTIES ('quality' = 'silver')
AS
WITH warehouse_daily_cost AS (
  -- Billed pool to allocate: cost per (day, workspace, warehouse) from the silver cost fact.
  SELECT
    usage_dt,
    account_cd,
    workspace_cd,
    usage_metadata.warehouse_id      AS warehouse_cd,
    SUM(line_cost_usd)               AS warehouse_cost_usd
  FROM global_it_hub.tf_billing_cost
  WHERE usage_metadata.warehouse_id IS NOT NULL
  GROUP BY usage_dt, account_cd, workspace_cd, usage_metadata.warehouse_id
),

query_base AS (
  -- One row per statement on a warehouse, with its billable weight.
  SELECT
    statement_cd,
    account_cd,
    workspace_cd,
    get_json_object(compute, '$.warehouse_id')             AS warehouse_cd,
    DATE(start_dt)                   AS usage_dt,
    executed_by_lb,
    executed_by_cd,
    statement_type_lb,
    execution_status_lb,
    client_application_lb,
    client_driver_lb,
    from_result_cache_fl,
    start_dt,
    end_dt,
    total_duration_ms_nb,
    waiting_for_compute_ms_nb,
    waiting_at_capacity_ms_nb,
    compilation_duration_ms_nb,
    execution_duration_ms_nb,
    CASE
      WHEN from_result_cache_fl = 1 THEN 0
      ELSE GREATEST(total_duration_ms_nb - COALESCE(waiting_at_capacity_ms_nb, 0), 1)
    END                              AS billable_duration_ms_nb,
    read_rows_nb,
    produced_rows_nb,
    read_bytes_nb,
    written_rows_nb,
    written_bytes_nb,
    get_json_object(query_source, '$.dashboard_id')        AS dashboard_cd,
    get_json_object(query_source, '$.notebook_id')         AS notebook_cd,
    get_json_object(query_source, '$.job_info.job_id')     AS job_cd,
    get_json_object(query_source, '$.genie_space_id')      AS genie_space_cd,
    query_tags
  FROM global_it_hub.tf_query_history
  WHERE get_json_object(compute, '$.warehouse_id') IS NOT NULL
),

daily_warehouse_totals AS (
  -- Denominator of the allocation fraction: total billable ms per (day, workspace, warehouse).
  SELECT
    usage_dt,
    account_cd,
    workspace_cd,
    warehouse_cd,
    SUM(billable_duration_ms_nb)     AS total_billable_ms_nb
  FROM query_base
  GROUP BY usage_dt, account_cd, workspace_cd, warehouse_cd
)

SELECT
  t1.statement_cd,
  t1.account_cd,
  t1.workspace_cd,
  t1.warehouse_cd,
  t1.usage_dt,
  t1.executed_by_lb,
  t1.executed_by_cd,
  t1.statement_type_lb,
  t1.execution_status_lb,
  t1.client_application_lb,
  t1.client_driver_lb,
  t1.from_result_cache_fl,
  t1.start_dt,
  t1.end_dt,
  t1.total_duration_ms_nb,
  t1.waiting_for_compute_ms_nb,
  t1.waiting_at_capacity_ms_nb,
  t1.compilation_duration_ms_nb,
  t1.execution_duration_ms_nb,
  t1.billable_duration_ms_nb,
  t1.read_rows_nb,
  t1.produced_rows_nb,
  t1.read_bytes_nb,
  t1.written_rows_nb,
  t1.written_bytes_nb,
  t1.dashboard_cd,
  t1.notebook_cd,
  t1.job_cd,
  t1.genie_space_cd,
  t1.query_tags,
  t3.warehouse_cost_usd,             -- billed pool for this warehouse-day (validation)
  t2.total_billable_ms_nb,           -- allocation denominator (validation)
  ROUND(
    CASE
      WHEN t1.from_result_cache_fl = 1   THEN 0
      WHEN t2.total_billable_ms_nb = 0   THEN 0
      ELSE t3.warehouse_cost_usd
           * (t1.billable_duration_ms_nb / t2.total_billable_ms_nb)
    END
  , 6)                               AS attributed_cost_usd
FROM query_base AS t1
INNER JOIN daily_warehouse_totals AS t2
  ON  t2.usage_dt      = t1.usage_dt
  AND t2.account_cd      = t1.account_cd
  AND t2.workspace_cd = t1.workspace_cd
  AND t2.warehouse_cd = t1.warehouse_cd
LEFT JOIN warehouse_daily_cost AS t3
  ON  t3.usage_dt      = t1.usage_dt
  AND t3.account_cd      = t1.account_cd
  AND t3.workspace_cd = t1.workspace_cd
  AND t3.warehouse_cd = t1.warehouse_cd;
