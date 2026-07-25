-- Databricks notebook source
-- DBTITLE 1,Notebook Summary
-- MAGIC %md
-- MAGIC ## Silver — User Security Bridge (recursive group flattening)
-- MAGIC
-- MAGIC Builds `global_it_hub.tr_dbx_user_security`: for every user in `td_dbx_user`, the flattened set
-- MAGIC of users/service principals they share visibility with —
-- MAGIC 1. themselves (always — covers users in no group at all),
-- MAGIC 2. every direct co-member (`Users`/`ServicePrincipals`) of each group they belong to,
-- MAGIC 3. every member of any group nested inside those groups, recursively, to arbitrary depth.
-- MAGIC
-- MAGIC Uses `WITH RECURSIVE` (Public Preview, requires DBR 17.0 / DBSQL 2025.20+) nested inside
-- MAGIC the `MERGE`'s `USING (...)` clause to compute the flattened set, then merges it into the
-- MAGIC target: new/reappeared `(user_cd, member_cd)` pairs are inserted, matched pairs are left
-- MAGIC untouched (no payload column beyond the key, so `sys_create_dt` stays as first-seen
-- MAGIC provenance), and pairs absent from the freshly recomputed source are hard-deleted
-- MAGIC (`WHEN NOT MATCHED BY SOURCE THEN DELETE`).
-- MAGIC
-- MAGIC Unfiltered by `td_dbx_user.is_active_fl` — deactivated users still get a row.
-- MAGIC
-- MAGIC | Source | Target |
-- MAGIC |---|---|
-- MAGIC | `td_dbx_user` × `tr_dbx_group_member` | `tr_dbx_user_security` |

-- COMMAND ----------

-- DBTITLE 1,Setup — widgets
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

-- DBTITLE 1,Step 1 — Recursively flatten group membership, resolve per-user visibility, merge
-- group_closure: group_cd -> every Users/ServicePrincipals member reachable from it, including
-- through arbitrarily nested Groups-type members. Base case = a group's own direct members;
-- recursive step propagates an already-resolved nested group's members up to every group that
-- nests it (join on tr_dbx_group_member.member_cd = the nested group's own group_cd).
-- user_direct_groups: groups each user directly belongs to (as a Users-type member).
-- group_based_visibility: for every group a user belongs to, they see that group's full closure.
-- self_rows: every real user sees themselves — always unioned in (see notebook summary).
-- The WITH RECURSIVE block sits inside USING (...) as a self-contained subquery, not prefixing
-- the MERGE statement — see notebook summary for why. Merge key: (user_cd, member_cd).
MERGE INTO global_it_hub.tr_dbx_user_security AS tgt
USING (
  WITH RECURSIVE group_closure (group_cd, member_cd) AS (
    SELECT group_cd, member_cd
    FROM global_it_hub.tr_dbx_group_member
    WHERE member_type_lb IN ('Users', 'ServicePrincipals')
      AND is_deleted_fl = 0

    UNION ALL

    SELECT t1.group_cd, t2.member_cd
    FROM global_it_hub.tr_dbx_group_member AS t1
    JOIN group_closure AS t2
      ON t1.member_cd = t2.group_cd
    WHERE t1.member_type_lb = 'Groups'
      AND t1.is_deleted_fl = 0
  ),
  user_direct_groups AS (
    SELECT group_cd, member_cd AS user_cd
    FROM global_it_hub.tr_dbx_group_member
    WHERE member_type_lb = 'Users'
      AND is_deleted_fl = 0
  ),
  group_based_visibility AS (
    SELECT t1.user_cd, t2.member_cd
    FROM user_direct_groups AS t1
    JOIN group_closure AS t2
      ON t1.group_cd = t2.group_cd
  ),
  self_rows AS (
    SELECT user_cd, user_cd AS member_cd
    FROM global_it_hub.td_dbx_user
    WHERE user_id > 0
  )
  SELECT DISTINCT user_cd, member_cd
  FROM (
    SELECT user_cd, member_cd FROM group_based_visibility
    UNION ALL
    SELECT user_cd, member_cd FROM self_rows
  )
) AS src
  ON  tgt.user_cd   = src.user_cd
  AND tgt.member_cd = src.member_cd
WHEN NOT MATCHED THEN INSERT (
  user_cd, member_cd, sys_create_dt, sys_update_dt
) VALUES (
  src.user_cd, src.member_cd, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
)
WHEN NOT MATCHED BY SOURCE THEN DELETE;
