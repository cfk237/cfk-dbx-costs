-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Workspace Dimension (SCD1)
-- MAGIC
-- MAGIC Loads `databricks.access_workspaces_latest` into `global_it_hub.td_workspace`.
-- MAGIC `access_workspaces_latest` is refreshed externally
-- MAGIC (`CREATE OR REPLACE TABLE ... AS SELECT * FROM system.access.workspaces_latest`) —
-- MAGIC a full snapshot every time, not append-only. That rules out a DLT streaming
-- MAGIC view/`apply_changes()` (would only ever capture the first snapshot and go stale
-- MAGIC afterward) and rules out an incremental watermark (there is nothing to be
-- MAGIC incremental about — every run sees every workspace).
-- MAGIC
-- MAGIC | Source | Target |
-- MAGIC |---|---|
-- MAGIC | `databricks.access_workspaces_latest` | `global_it_hub.td_workspace` |
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC Single `MERGE`, no watermark:
-- MAGIC - `WHEN MATCHED` — update name/URL/status on change; also clears `is_deleted_fl`
-- MAGIC   if a previously-deleted workspace reappears.
-- MAGIC - `WHEN NOT MATCHED BY TARGET` — insert new workspaces.
-- MAGIC - `WHEN NOT MATCHED BY SOURCE` — a workspace present in `td_workspace` but absent
-- MAGIC   from the latest snapshot is flagged `is_deleted_fl = 1` rather than removed.
-- MAGIC   Guarded by `workspace_id > 0` so sentinel rows (-4..-1), which never appear in
-- MAGIC   the source snapshot, are never touched by this branch.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog_env", "dev")
-- MAGIC catalog_env = dbutils.widgets.get("catalog_env")
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

-- COMMAND ----------

-- DBTITLE 1,Step 1 — Upsert td_workspace, flag missing workspaces as deleted
MERGE INTO global_it_hub.td_workspace AS tgt
USING IDENTIFIER(:bronze_catalog_name || '_' || :catalog_env || '.' || :bronze_schema || '.' || 'access_workspaces_latest') AS src
  ON  tgt.account_cd      = src.account_id
  AND tgt.workspace_cd = src.workspace_id
WHEN MATCHED AND (
     tgt.workspace_lb IS DISTINCT FROM src.workspace_name
  OR tgt.workspace_url_lb  IS DISTINCT FROM src.workspace_url
  OR tgt.status_lb         IS DISTINCT FROM src.status
  OR tgt.is_deleted_fl     = 1
) THEN UPDATE SET
  tgt.workspace_lb = src.workspace_name,
  tgt.workspace_url_lb  = src.workspace_url,
  tgt.status_lb         = src.status,
  tgt.is_deleted_fl     = 0,
  tgt.sys_update_dt     = CURRENT_TIMESTAMP()
WHEN NOT MATCHED BY TARGET THEN INSERT (
  account_cd, workspace_cd, workspace_lb, workspace_url_lb, create_dt, status_lb,
  is_deleted_fl, sys_create_dt, sys_update_dt
) VALUES (
  src.account_id, src.workspace_id, src.workspace_name, src.workspace_url, src.create_time, src.status,
  0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
)
WHEN NOT MATCHED BY SOURCE AND tgt.workspace_id > 0 AND tgt.is_deleted_fl = 0 THEN UPDATE SET
  tgt.is_deleted_fl = 1,
  tgt.sys_update_dt = CURRENT_TIMESTAMP();
