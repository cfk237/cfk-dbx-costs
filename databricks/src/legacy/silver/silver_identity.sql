-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Identity (Users & Service Principals, SCD1)
-- MAGIC
-- MAGIC Incrementally loads identity data from two SAL tables into three silver targets.
-- MAGIC
-- MAGIC | Target | Source | Key |
-- MAGIC |---|---|---|
-- MAGIC | `global_it_hub.td_user` | `databricks.sal_users` | `(account_id, user_id_cd)` |
-- MAGIC | `global_it_hub.td_service_principal` | `databricks.sal_service_principals` | `(account_id, sp_id_cd)` |
-- MAGIC | `global_it_hub.td_identity` | `v_affected_users` ∪ `v_affected_sps` | `(account_id, source_id_cd, identity_type_lb)` |
-- MAGIC
-- MAGIC **Prerequisites:** run `raw_extract_api_identities.py` → `sal_ingest_api_identities.py`
-- MAGIC to refresh the SAL staging tables.
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC 1. Read watermark from `_pipeline_watermarks`
-- MAGIC 2. Stage affected users from `sal_users` — deduplicated to latest snapshot per user
-- MAGIC 3. Stage affected service principals from `sal_service_principals` — same deduplication
-- MAGIC 4. Upsert `td_user` from staged users
-- MAGIC 5. Upsert `td_service_principal` from staged service principals
-- MAGIC 6. Upsert `td_identity` directly from staged records (no re-scan of silver tables)
-- MAGIC 7. Advance watermark to `MAX(sal_load_dt)` of processed records

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
WHERE table_name = 'global_it_hub.td_identity'
  AND account_id  = '*';

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Stage affected users
-- Filters to SAL records newer than the watermark; deduplicates to the latest snapshot
-- per user — sal_users accumulates full snapshots on each extract run.
CREATE OR REPLACE TEMPORARY TABLE v_affected_users AS
SELECT
  account_id,
  user_id_cd,
  username_lb,
  display_name_lb,
  first_name_lb,
  last_name_lb,
  is_active_fl,
  external_id_cd,
  primary_email_lb,
  sal_load_dt,
  CURRENT_TIMESTAMP() AS sys_update_dt
FROM databricks.sal_users
WHERE sal_load_dt > (SELECT last_processed_dt FROM v_watermark)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY account_id, user_id_cd
  ORDER BY sal_load_dt DESC
) = 1;

-- COMMAND ----------

-- DBTITLE 1,Step 3 — Stage affected service principals
-- Same pattern as Step 2 — filters and deduplicates sal_service_principals.
CREATE OR REPLACE TEMPORARY TABLE v_affected_sps AS
SELECT
  account_id,
  sp_id_cd,
  application_id_cd,
  display_name_lb,
  is_active_fl,
  external_id_cd,
  sal_load_dt,
  CURRENT_TIMESTAMP() AS sys_update_dt
FROM databricks.sal_service_principals
WHERE sal_load_dt > (SELECT last_processed_dt FROM v_watermark)
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY account_id, sp_id_cd
  ORDER BY sal_load_dt DESC
) = 1;

-- COMMAND ----------

-- DBTITLE 1,Step 4 — Upsert td_user
-- Merge key: (account_id, user_id_cd).
-- UPDATE fires only when at least one attribute has changed (IS DISTINCT FROM guards).
MERGE INTO global_it_hub.td_user AS tgt
USING v_affected_users             AS src
  ON  tgt.account_id = src.account_id
  AND tgt.user_id_cd = src.user_id_cd
WHEN MATCHED AND (
     tgt.username_lb      IS DISTINCT FROM src.username_lb
  OR tgt.display_name_lb  IS DISTINCT FROM src.display_name_lb
  OR tgt.first_name_lb    IS DISTINCT FROM src.first_name_lb
  OR tgt.last_name_lb     IS DISTINCT FROM src.last_name_lb
  OR tgt.is_active_fl     IS DISTINCT FROM src.is_active_fl
  OR tgt.external_id_cd   IS DISTINCT FROM src.external_id_cd
  OR tgt.primary_email_lb IS DISTINCT FROM src.primary_email_lb
) THEN UPDATE SET
  tgt.username_lb      = src.username_lb,
  tgt.display_name_lb  = src.display_name_lb,
  tgt.first_name_lb    = src.first_name_lb,
  tgt.last_name_lb     = src.last_name_lb,
  tgt.is_active_fl     = src.is_active_fl,
  tgt.external_id_cd   = src.external_id_cd,
  tgt.primary_email_lb = src.primary_email_lb,
  tgt.sys_update_dt    = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, user_id_cd, username_lb, display_name_lb,
  first_name_lb, last_name_lb, is_active_fl,
  external_id_cd, primary_email_lb,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.user_id_cd, src.username_lb, src.display_name_lb,
  src.first_name_lb, src.last_name_lb, src.is_active_fl,
  src.external_id_cd, src.primary_email_lb,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 5 — Upsert td_service_principal
-- Merge key: (account_id, sp_id_cd).
MERGE INTO global_it_hub.td_service_principal AS tgt
USING v_affected_sps                           AS src
  ON  tgt.account_id = src.account_id
  AND tgt.sp_id_cd   = src.sp_id_cd
WHEN MATCHED AND (
     tgt.application_id_cd IS DISTINCT FROM src.application_id_cd
  OR tgt.display_name_lb   IS DISTINCT FROM src.display_name_lb
  OR tgt.is_active_fl      IS DISTINCT FROM src.is_active_fl
  OR tgt.external_id_cd    IS DISTINCT FROM src.external_id_cd
) THEN UPDATE SET
  tgt.application_id_cd = src.application_id_cd,
  tgt.display_name_lb   = src.display_name_lb,
  tgt.is_active_fl      = src.is_active_fl,
  tgt.external_id_cd    = src.external_id_cd,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, sp_id_cd, application_id_cd, display_name_lb,
  is_active_fl, external_id_cd,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.sp_id_cd, src.application_id_cd, src.display_name_lb,
  src.is_active_fl, src.external_id_cd,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 6 — Upsert td_identity
-- Sourced directly from the staged temp tables — no re-scan of td_user / td_service_principal.
-- Merge key: (account_id, source_id_cd, identity_type_lb).
-- identity_id_cd: natural login identifier — username for users, application (client) ID for
-- service principals. Used downstream to join billing usage run_as_cd (vf_pbi_billing_cost).
-- UNION ALL matches columns by position, not name — both branches below must list columns
-- in the exact same order.
MERGE INTO global_it_hub.td_identity AS tgt
USING (
  SELECT
    account_id,
    user_id_cd           AS source_id_cd,
    'USER'               AS identity_type_lb,
    display_name_lb,
    username_lb           AS identity_id_cd,
    first_name_lb,
    last_name_lb,
    primary_email_lb,
    is_active_fl,
    external_id_cd,
    sys_update_dt
  FROM v_affected_users

  UNION ALL

  SELECT
    account_id,
    sp_id_cd               AS source_id_cd,
    'SERVICE PRINCIPAL'    AS identity_type_lb,
    display_name_lb,
    application_id_cd      AS identity_id_cd,
    CAST(NULL AS STRING)   AS first_name_lb,
    CAST(NULL AS STRING)   AS last_name_lb,
    CAST(NULL AS STRING)   AS primary_email_lb,
    is_active_fl,
    external_id_cd,
    sys_update_dt
  FROM v_affected_sps
) AS src
  ON  tgt.account_id       = src.account_id
  AND tgt.source_id_cd     = src.source_id_cd
  AND tgt.identity_type_lb = src.identity_type_lb
WHEN MATCHED AND (
     tgt.display_name_lb  IS DISTINCT FROM src.display_name_lb
  OR tgt.identity_id_cd    IS DISTINCT FROM src.identity_id_cd
  OR tgt.first_name_lb     IS DISTINCT FROM src.first_name_lb
  OR tgt.last_name_lb      IS DISTINCT FROM src.last_name_lb
  OR tgt.primary_email_lb  IS DISTINCT FROM src.primary_email_lb
  OR tgt.is_active_fl      IS DISTINCT FROM src.is_active_fl
  OR tgt.external_id_cd    IS DISTINCT FROM src.external_id_cd
) THEN UPDATE SET
  tgt.display_name_lb  = src.display_name_lb,
  tgt.identity_id_cd    = src.identity_id_cd,
  tgt.first_name_lb     = src.first_name_lb,
  tgt.last_name_lb      = src.last_name_lb,
  tgt.primary_email_lb  = src.primary_email_lb,
  tgt.is_active_fl      = src.is_active_fl,
  tgt.external_id_cd    = src.external_id_cd,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (
  account_id, source_id_cd, identity_type_lb, display_name_lb,
  identity_id_cd, first_name_lb, last_name_lb, primary_email_lb,
  is_active_fl, external_id_cd,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.source_id_cd, src.identity_type_lb, src.display_name_lb,
  src.identity_id_cd, src.first_name_lb, src.last_name_lb, src.primary_email_lb,
  src.is_active_fl, src.external_id_cd,
  src.sys_update_dt, src.sys_update_dt
);

-- COMMAND ----------

-- DBTITLE 1,Step 7 — Advance watermark
-- MAX(sal_load_dt) from both staged tables ensures the watermark reflects the highest
-- load timestamp across users and service principals.
MERGE INTO global_it_hub._pipeline_watermarks AS tgt
USING (
  SELECT
    'global_it_hub.td_identity' AS table_name,
    '*'                          AS account_id,
    COALESCE(
      GREATEST(
        MAX(u.sal_load_dt),
        MAX(s.sal_load_dt)
      ),
      (SELECT last_processed_dt FROM v_watermark)
    )                            AS last_processed_dt,
    CURRENT_TIMESTAMP()          AS sys_update_dt
  FROM v_affected_users AS u
  CROSS JOIN (SELECT MAX(sal_load_dt) AS sal_load_dt FROM v_affected_sps) AS s
) AS src
  ON  tgt.table_name = src.table_name
  AND tgt.account_id  = src.account_id
WHEN MATCHED THEN UPDATE SET
  tgt.last_processed_dt = src.last_processed_dt,
  tgt.sys_update_dt     = src.sys_update_dt
WHEN NOT MATCHED THEN INSERT (table_name, account_id, last_processed_dt, sys_update_dt)
  VALUES (src.table_name, src.account_id, src.last_processed_dt, src.sys_update_dt);
