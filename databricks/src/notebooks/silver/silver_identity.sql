-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — Identity Dimension (Users & Service Principals, SCD1)
-- MAGIC
-- MAGIC Loads account-level identities into `td_dbx_user`, `td_dbx_service_principal`, and the
-- MAGIC unified `td_identity`.
-- MAGIC
-- MAGIC **Source:** `account_users` / `account_service_principals` — populated by an Account
-- MAGIC API export job (`export_principals.py`) via row-level upsert + soft-delete (one row per
-- MAGIC id; a row missing from a later API pull gets `deleted_at` set rather than being
-- MAGIC physically removed). That refresh pattern rules out a DLT streaming view/`apply_changes()`
-- MAGIC — same reasoning as `td_workspace`/`td_billing_list_price`: a row-level MERGE source is
-- MAGIC not append-only, so `skipChangeCommits=true` would only ever capture each row's first
-- MAGIC snapshot and never see later updates.
-- MAGIC
-- MAGIC | Source | Target |
-- MAGIC |---|---|
-- MAGIC | `{bronze_schema}.account_users` | `global_it_hub.td_dbx_user` |
-- MAGIC | `{bronze_schema}.account_service_principals` | `global_it_hub.td_dbx_service_principal` |
-- MAGIC | (union of both, staged) | `global_it_hub.td_identity` |
-- MAGIC
-- MAGIC ### Field remapping
-- MAGIC Both source tables carry only a handful of scalar columns (`id`, `account_id` — aliased
-- MAGIC to `account_cd` on the way in, since `td_dbx_user`/`td_dbx_service_principal`/`td_identity` use
-- MAGIC the project's `_cd` natural-key convention — `user_name`/`application_id`, `display_name`,
-- MAGIC `active`, `deleted_at`) plus a `raw` SCIM JSON payload. The remaining `td_dbx_user`/`td_identity`
-- MAGIC columns are parsed out of `raw`:
-- MAGIC - `first_name_lb` / `last_name_lb` ← `raw.name.givenName` / `raw.name.familyName` (users only)
-- MAGIC - `external_cd` ← `raw.externalId`
-- MAGIC - `primary_email_lb` ← the entry in `raw.emails[]` with `primary = true` (users only)
-- MAGIC
-- MAGIC `is_active_fl` folds in the source's soft-delete: `1` only when `active = true` **and**
-- MAGIC `deleted_at IS NULL` — an id absent from a later API pull is treated the same as a
-- MAGIC disabled one (no separate `is_deleted_fl`, unlike `td_workspace`).
-- MAGIC
-- MAGIC The standard audit columns are driven directly by the source's own timestamps:
-- MAGIC `sys_create_dt` ← `first_seen_at`, `sys_update_dt` ← `last_seen_at`.
-- MAGIC
-- MAGIC ### Processing strategy
-- MAGIC No watermark: both source tables are already deduplicated to one row per id (upsert-
-- MAGIC maintained), so target vs. source is compared directly every run — same pattern as
-- MAGIC `silver_workspace.sql`. `v_account_users`/`v_account_service_principals` are staged once
-- MAGIC as temp tables (each referenced twice — once to MERGE its own `td_*` table, once in the
-- MAGIC `td_identity` union) so the `raw` JSON is parsed only once per source row.

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

-- DBTITLE 1,Step 1 — Stage users (remap columns, parse raw JSON)
CREATE OR REPLACE TEMPORARY TABLE v_account_users AS
SELECT
  id                                                        AS user_cd,
  account_id                                                AS account_cd,
  get_json_object(raw, '$.userName')                        AS username_lb,
  get_json_object(raw, '$.displayName')                     AS display_name_lb,
  get_json_object(raw, '$.name.givenName')                  AS first_name_lb,
  get_json_object(raw, '$.name.familyName')                 AS last_name_lb,
  CASE WHEN active AND deleted_at IS NULL THEN 1 ELSE 0 END AS is_active_fl,
  get_json_object(raw, '$.externalId')                      AS external_cd,
  element_at(
    filter(
      from_json(get_json_object(raw, '$.emails'), 'array<struct<primary:boolean,type:string,value:string>>'),
      e -> e.primary = true
    ),
    1
  ).value                                                   AS primary_email_lb,
  first_seen_at,
  last_seen_at
FROM IDENTIFIER(:bronze_catalog_name || '_' || :catalog_env || '.' || :bronze_schema || '.' || 'account_users');

-- COMMAND ----------

-- DBTITLE 1,Step 2 — Stage service principals (remap columns, parse raw JSON)
CREATE OR REPLACE TEMPORARY TABLE v_account_service_principals AS
SELECT
  id                                                        AS sp_cd,
  account_id                                                AS account_cd,
  get_json_object(raw, '$.applicationId')                   AS application_cd,
  get_json_object(raw, '$.displayName')                     AS display_name_lb,
  CASE WHEN active AND deleted_at IS NULL THEN 1 ELSE 0 END AS is_active_fl,
  get_json_object(raw, '$.externalId')                      AS external_cd,
  first_seen_at,
  last_seen_at
FROM IDENTIFIER(:bronze_catalog_name || '_' || :catalog_env || '.' || :bronze_schema || '.' || 'account_service_principals');

-- COMMAND ----------

-- DBTITLE 1,Step 3 — Upsert td_dbx_user
-- Merge key: (account_cd, user_cd).
MERGE INTO global_it_hub.td_dbx_user AS tgt
USING v_account_users AS src
  ON  tgt.account_cd = src.account_cd
  AND tgt.user_cd = src.user_cd
WHEN MATCHED AND (
     tgt.username_lb      IS DISTINCT FROM src.username_lb
  OR tgt.display_name_lb  IS DISTINCT FROM src.display_name_lb
  OR tgt.first_name_lb    IS DISTINCT FROM src.first_name_lb
  OR tgt.last_name_lb     IS DISTINCT FROM src.last_name_lb
  OR tgt.is_active_fl     IS DISTINCT FROM src.is_active_fl
  OR tgt.external_cd   IS DISTINCT FROM src.external_cd
  OR tgt.primary_email_lb IS DISTINCT FROM src.primary_email_lb
) THEN UPDATE SET
  tgt.username_lb      = src.username_lb,
  tgt.display_name_lb  = src.display_name_lb,
  tgt.first_name_lb    = src.first_name_lb,
  tgt.last_name_lb     = src.last_name_lb,
  tgt.is_active_fl     = src.is_active_fl,
  tgt.external_cd   = src.external_cd,
  tgt.primary_email_lb = src.primary_email_lb,
  tgt.sys_update_dt    = src.last_seen_at
WHEN NOT MATCHED THEN INSERT (
  account_cd, user_cd, username_lb, display_name_lb,
  first_name_lb, last_name_lb, is_active_fl,
  external_cd, primary_email_lb,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_cd, src.user_cd, src.username_lb, src.display_name_lb,
  src.first_name_lb, src.last_name_lb, src.is_active_fl,
  src.external_cd, src.primary_email_lb,
  src.first_seen_at, src.last_seen_at
);

-- COMMAND ----------

-- DBTITLE 1,Step 4 — Upsert td_dbx_service_principal
-- Merge key: (account_cd, sp_cd).
MERGE INTO global_it_hub.td_dbx_service_principal AS tgt
USING v_account_service_principals AS src
  ON  tgt.account_cd = src.account_cd
  AND tgt.sp_cd   = src.sp_cd
WHEN MATCHED AND (
     tgt.application_cd IS DISTINCT FROM src.application_cd
  OR tgt.display_name_lb   IS DISTINCT FROM src.display_name_lb
  OR tgt.is_active_fl      IS DISTINCT FROM src.is_active_fl
  OR tgt.external_cd    IS DISTINCT FROM src.external_cd
) THEN UPDATE SET
  tgt.application_cd = src.application_cd,
  tgt.display_name_lb   = src.display_name_lb,
  tgt.is_active_fl      = src.is_active_fl,
  tgt.external_cd    = src.external_cd,
  tgt.sys_update_dt     = src.last_seen_at
WHEN NOT MATCHED THEN INSERT (
  account_cd, sp_cd, application_cd, display_name_lb,
  is_active_fl, external_cd,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_cd, src.sp_cd, src.application_cd, src.display_name_lb,
  src.is_active_fl, src.external_cd,
  src.first_seen_at, src.last_seen_at
);

-- COMMAND ----------

-- DBTITLE 1,Step 5 — Upsert td_identity (union of staged users + service principals)
-- Merge key: (account_cd, source_cd, identity_type_lb). Sourced directly from the staged
-- temp tables — no re-scan of td_dbx_user / td_dbx_service_principal.
-- identity_cd: natural login identifier — username for users, application (client) ID for
-- service principals. Used downstream to join billing usage run_as_cd (vf_pbi_billing_cost).
-- UNION ALL matches columns by position, not name — both branches must list columns in the
-- exact same order.
MERGE INTO global_it_hub.td_identity AS tgt
USING (
  SELECT
    account_cd,
    user_cd            AS source_cd,
    'USER'                AS identity_type_lb,
    display_name_lb,
    username_lb            AS identity_cd,
    first_name_lb,
    last_name_lb,
    primary_email_lb,
    is_active_fl,
    external_cd,
    first_seen_at,
    last_seen_at
  FROM v_account_users

  UNION ALL

  SELECT
    account_cd,
    sp_cd               AS source_cd,
    'SERVICE PRINCIPAL'    AS identity_type_lb,
    display_name_lb,
    application_cd      AS identity_cd,
    CAST(NULL AS STRING)   AS first_name_lb,
    CAST(NULL AS STRING)   AS last_name_lb,
    CAST(NULL AS STRING)   AS primary_email_lb,
    is_active_fl,
    external_cd,
    first_seen_at,
    last_seen_at
  FROM v_account_service_principals
) AS src
  ON  tgt.account_cd       = src.account_cd
  AND tgt.source_cd     = src.source_cd
  AND tgt.identity_type_lb = src.identity_type_lb
WHEN MATCHED AND (
     tgt.display_name_lb  IS DISTINCT FROM src.display_name_lb
  OR tgt.identity_cd    IS DISTINCT FROM src.identity_cd
  OR tgt.first_name_lb     IS DISTINCT FROM src.first_name_lb
  OR tgt.last_name_lb      IS DISTINCT FROM src.last_name_lb
  OR tgt.primary_email_lb  IS DISTINCT FROM src.primary_email_lb
  OR tgt.is_active_fl      IS DISTINCT FROM src.is_active_fl
  OR tgt.external_cd    IS DISTINCT FROM src.external_cd
) THEN UPDATE SET
  tgt.display_name_lb  = src.display_name_lb,
  tgt.identity_cd    = src.identity_cd,
  tgt.first_name_lb     = src.first_name_lb,
  tgt.last_name_lb      = src.last_name_lb,
  tgt.primary_email_lb  = src.primary_email_lb,
  tgt.is_active_fl      = src.is_active_fl,
  tgt.external_cd    = src.external_cd,
  tgt.sys_update_dt     = src.last_seen_at
WHEN NOT MATCHED THEN INSERT (
  account_cd, source_cd, identity_type_lb, display_name_lb,
  identity_cd, first_name_lb, last_name_lb, primary_email_lb,
  is_active_fl, external_cd,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_cd, src.source_cd, src.identity_type_lb, src.display_name_lb,
  src.identity_cd, src.first_name_lb, src.last_name_lb, src.primary_email_lb,
  src.is_active_fl, src.external_cd,
  src.first_seen_at, src.last_seen_at
);

-- COMMAND ----------

-- DBTITLE 1,Step 6 — Upsert td_dbx_group
-- Source: account_groups — full snapshot refreshed externally, same as account_users/
-- account_service_principals. Groups have no SCIM "active" attribute (unlike users/service
-- principals), so is_active_fl is derived from deleted_at alone.
-- Read inline (no temp-table staging): unlike users/service principals, td_dbx_group and
-- tr_dbx_group_member need different parts of "raw" (displayName vs. members), so there is no
-- shared parse to save by staging — and staging into a temp table breaks Unity Catalog table
-- lineage back to account_groups (temp tables have no catalog identity to attach a lineage
-- node to), the same gap diagnosed for td_dbx_service_principal/td_dbx_user via v_account_*.
-- Merge key: (account_cd, group_cd).
MERGE INTO global_it_hub.td_dbx_group AS tgt
USING (
  SELECT
    id                                     AS group_cd,
    account_id                             AS account_cd,
    get_json_object(raw, '$.displayName')  AS display_name_lb,
    CASE WHEN deleted_at IS NULL THEN 1 ELSE 0 END AS is_active_fl,
    first_seen_at,
    last_seen_at
  FROM IDENTIFIER(:bronze_catalog_name || '_' || :catalog_env || '.' || :bronze_schema || '.' || 'account_groups')
) AS src
  ON  tgt.account_cd = src.account_cd
  AND tgt.group_cd   = src.group_cd
WHEN MATCHED AND (
     tgt.display_name_lb IS DISTINCT FROM src.display_name_lb
  OR tgt.is_active_fl     IS DISTINCT FROM src.is_active_fl
) THEN UPDATE SET
  tgt.display_name_lb = src.display_name_lb,
  tgt.is_active_fl     = src.is_active_fl,
  tgt.sys_update_dt    = src.last_seen_at
WHEN NOT MATCHED THEN INSERT (
  account_cd, group_cd, display_name_lb, is_active_fl,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_cd, src.group_cd, src.display_name_lb, src.is_active_fl,
  src.first_seen_at, src.last_seen_at
);

-- COMMAND ----------

-- DBTITLE 1,Step 7 — Upsert tr_dbx_group_member
-- Explodes account_groups.raw.members into one row per (group, member) edge. member_type_lb/
-- member_cd are parsed from the SCIM $ref path (e.g. "Groups/84156647790198" -> type "Groups",
-- id "84156647790198"); member_cd may reference a user, service principal, or another group
-- depending on member_type_lb — resolve against td_dbx_user/td_dbx_service_principal/td_dbx_group
-- downstream using member_type_lb as the discriminator.
-- account_groups is a full snapshot every run (same as Step 6), so this MERGE flags stale rows
-- rather than deleting them — same pattern as td_workspace (silver_workspace.sql): a member
-- removed from a group (or whose group was soft-deleted, filtered out by the WHERE below) gets
-- is_deleted_fl = 1 instead of being physically removed; a member that reappears in a later run
-- clears the flag back to 0. This assumes src always covers every current group's membership —
-- true as long as the source stays a full snapshot.
-- Merge key: (account_cd, group_cd, member_cd).
MERGE INTO global_it_hub.tr_dbx_group_member AS tgt
USING (
  SELECT
    account_id                          AS account_cd,
    id                                  AS group_cd,
    element_at(split(member.ref, '/'), 1)   AS member_type_lb,
    element_at(split(member.ref, '/'), -1)  AS member_cd,
    last_seen_at
  FROM IDENTIFIER(:bronze_catalog_name || '_' || :catalog_env || '.' || :bronze_schema || '.' || 'account_groups')
  LATERAL VIEW explode(
    from_json(
      -- Renaming the JSON key $ref -> ref before parsing, instead of declaring a backtick-quoted
      -- `$ref` field in the from_json schema: on Serverless SQL, extracting a field named `$ref`
      -- via from_json's schema-DDL-string parser intermittently produced NULL even though the
      -- source JSON always had a non-null value there (confirmed by direct query) — suspected
      -- engine-version difference in how that parser handles `$` in a backtick-quoted field name.
      -- Stripping the `$` sidesteps it entirely without changing the rest of the derivation logic.
      REPLACE(get_json_object(raw, '$.members'), '$ref', 'ref'),
      'array<struct<ref:string,display:string,value:string>>'
    )
  ) exploded_members AS member
  WHERE deleted_at IS NULL
) AS src
  ON  tgt.account_cd = src.account_cd
  AND tgt.group_cd   = src.group_cd
  AND tgt.member_cd  = src.member_cd
WHEN MATCHED AND (
     tgt.member_type_lb IS DISTINCT FROM src.member_type_lb
  OR tgt.is_deleted_fl   = 1
) THEN UPDATE SET
  tgt.member_type_lb = src.member_type_lb,
  tgt.is_deleted_fl   = 0,
  tgt.sys_update_dt   = src.last_seen_at
WHEN NOT MATCHED THEN INSERT (
  account_cd, group_cd, member_cd, member_type_lb, is_deleted_fl,
  sys_create_dt, sys_update_dt
) VALUES (
  src.account_cd, src.group_cd, src.member_cd, src.member_type_lb, 0,
  src.last_seen_at, src.last_seen_at
)
WHEN NOT MATCHED BY SOURCE AND tgt.is_deleted_fl = 0 THEN UPDATE SET
  tgt.is_deleted_fl = 1,
  tgt.sys_update_dt = CURRENT_TIMESTAMP();
