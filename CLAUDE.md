# CLAUDE.md — cfk-databricks-costs

Project-level conventions for Claude Code. Apply these rules automatically when writing or modifying any file in this repository.

---

## Project overview

Databricks cost monitoring project. Ingests Databricks system tables and SCIM API data into a silver medallion layer for cost attribution and identity reporting.

---

## Catalog & schema layout

| Layer | Catalog | Schema | Prefix |
|---|---|---|---|
| Staging (API) | `{catalog_name}_{catalog_env}` | `databricks` | `sal_` |
| Silver — dimension | `{catalog_name}_{catalog_env}` | `global_it_hub` | `td_` |
| Silver — fact | `{catalog_name}_{catalog_env}` | `global_it_hub` | `tf_` |
| Silver — bridge/relation | `{catalog_name}_{catalog_env}` | `global_it_hub` | `tr_` |
| Gold — table | `{catalog_name}_{catalog_env}` | `global_it_hub` | `tg_` |
| Gold — PBI dimension view | `{catalog_name}_{catalog_env}` | `dataplatform_usage` | `vd_pbi_` |
| Gold — PBI fact view | `{catalog_name}_{catalog_env}` | `dataplatform_usage` | `vf_pbi_` |
| Gold — PBI bridge view | `{catalog_name}_{catalog_env}` | `dataplatform_usage` | `vr_pbi_` |

Default widget values: `catalog_name = it_security`, `catalog_env = dev`.

Every notebook setup cell follows this pattern:
```python
dbutils.widgets.text("catalog_env", "dev")
catalog_env = dbutils.widgets.get("catalog_env")
dbutils.widgets.text("catalog_name", "it_security")
catalog_name = dbutils.widgets.get("catalog_name")
catalog = f"{catalog_name}_{catalog_env}"
```

---

## Column naming convention

| Suffix | Type | Meaning |
|---|---|---|
| `_lb` | STRING | Label / display name / free text |
| `_cd` | STRING | Code / identifier / key (including UUIDs) |
| `_dt` | TIMESTAMP | Date or datetime |
| `_fl` | INT | Flag / boolean (0/1, never actual BOOLEAN) |
| `_nb` | BIGINT/INT | Numeric / count / measure |

Natural keys from source system tables are always renamed to end in `_cd` by replacing a trailing `_id` with `_cd` (e.g. `account_id` → `account_cd`).

---

## Table naming convention

- **Singular** entity names: `td_lakeflow_job`, `td_compute_cluster`, `td_dbx_user` (not `td_jobs`, `td_clusters`)
- SCD2 history tables keep `_history` suffix: `td_lakeflow_job_history`
- Current-state tables have no suffix: `td_lakeflow_job`
- Staging API tables use `sal_` prefix: `sal_users`, `sal_service_principals`

---

## SQL alias conventions

Apply these rules to every SQL statement:

| Context | Rule |
|---|---|
| `SELECT` with ≥1 `JOIN` in the FROM clause, **or** a correlated `EXISTS`/`NOT EXISTS` subquery referencing another table | Alias tables `t1`, `t2`, … in order of first appearance |
| `MERGE` statement | Target = `tgt`, USING source = `src` (always, regardless of JOIN count) |
| `SELECT` with no `JOIN` and no correlated subquery (simple FROM … WHERE) | Keep short descriptive aliases; no forced renaming |

When a MERGE contains a subquery in its USING clause that itself has a JOIN, the **inner subquery** uses `t1`/`t2` and the **outer MERGE** uses `tgt`/`src`. These are different scopes and do not conflict. The same applies to a plain (non-MERGE) `SELECT` whose `WHERE` clause contains a correlated `EXISTS`/`NOT EXISTS` subquery: the outer query's table gets `t1`, and each table introduced inside the subquery continues the same `t1`/`t2`/… numbering — one counter across the whole statement, not reset per subquery.

---

## SCD2 load notebook pattern

Each silver load notebook follows these six steps exactly:

1. **Step 1** — Read watermark (`CREATE OR REPLACE TEMP VIEW v_watermark`)
2. **Step 2** — Identify affected entities (`CREATE OR REPLACE TEMPORARY TABLE v_affected_{entity}`)
3. **Step 3** — Compute SCD2 validity windows (`CREATE OR REPLACE TEMP VIEW v_scd2_source`)
4. **Step 4** — Upsert history table (`MERGE INTO td_{entity}_history`)
5. **Step 5** — Upsert current table (`MERGE INTO td_{entity}`)
6. **Step 6** — Advance watermark (`MERGE INTO _pipeline_watermarks`)

Key invariants:
- `v_affected_{entity}` is a **`CREATE OR REPLACE TEMPORARY TABLE`** — referenced in 3 separate statements (Steps 3, 5, 6), materialized once to avoid triple source scan; `OR REPLACE` makes re-runs within the same session safe (DBR 13.3+).
- `v_scd2_source` is a **temp view** — referenced only once (Step 4).
- Watermark is advanced **last** (Step 6) to guarantee idempotency on re-run.
- Step 6 watermark MERGE uses a **scalar subquery** `(SELECT last_processed_dt FROM v_watermark)` as the COALESCE fallback — **not a CROSS JOIN**. When `v_affected_{entity}` is empty the INNER JOIN produces 0 rows; a CROSS JOIN with 0 rows also produces 0 rows, so `MAX()` returns NULL. The scalar subquery is evaluated independently of the FROM clause and always returns the existing watermark value.
- `_pipeline_watermarks` composite key is `(table_name, account_id)`. **SAL ingest rows** use the actual account UUID; **silver load rows** use `'*'` (all-accounts sentinel). Always include `AND account_id = '*'` in silver watermark reads and `'*' AS account_id` in silver watermark advances.
- `sys_create_dt = sys_update_dt` on INSERT (same `CURRENT_TIMESTAMP()` reference via `src.sys_update_dt`).
- History MERGE key: `(workspace_id, {entity}_cd, valid_from_dt)`.
- Current MERGE key: `(workspace_id, {entity}_cd)`.

---

## Surrogate keys

Silver dimension tables that feed Power BI use integer surrogate keys:

```sql
entity_id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1)
```

`BY DEFAULT` (not `ALWAYS`) is required so sentinel rows (`-1`, `-2`) can be inserted explicitly. The surrogate key is **omitted** from MERGE INSERT column lists — Delta assigns it automatically.

Sentinel rows in every dimension table:
- `-2` = Unassigned (`'UNS'` for all code/label fields)
- `-1` = Unknown (`'UNK'` for all code/label fields)

Sentinel INSERT is wrapped in a `MERGE … WHEN NOT MATCHED` to make `init_tables_silver.sql` fully idempotent.

---

## SQL widget parameter syntax

In SQL cells, always reference widget values with the **`:param`** syntax — not the legacy `${param}`:

```sql
-- Correct (Databricks SQL current syntax)
WHERE ingestion_date >= current_date() - :interval_days

-- Deprecated (old ${} syntax — avoid)
WHERE ingestion_date >= current_date() - ${interval_days}
```

The `:param` form works in both SQL warehouse and cluster notebook SQL cells (DBR 13.3+) and is required by Databricks SQL serverless. Widget values are still defined in Python cells via `dbutils.widgets.text(...)`.

---

## Databricks notebook format

SQL notebooks (`.sql`):
```
-- Databricks notebook source
-- DBTITLE 1,Cell title
-- MAGIC %md / %python / %sql
-- COMMAND ----------
```

Python notebooks (`.py`):
```
# Databricks notebook source
# DBTITLE 1,Cell title
# MAGIC %md
# COMMAND ----------
```

Auth in Python notebooks always uses notebook context (no hard-coded tokens):
```python
ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host  = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}"}
```

---

## T-SQL linter false positives

VS Code's T-SQL linter flags valid Spark SQL syntax as errors. These are cosmetic and can be ignored:
- `CREATE TABLE IF NOT EXISTS`
- `CREATE OR REPLACE TEMP VIEW`
- `CREATE TEMPORARY TABLE`
- `LEAD() OVER (PARTITION BY …)`
- `MERGE … USING … ON` (Spark MERGE syntax)
- `CAST(NULL AS STRING)`
- `QUALIFY` (post-`WHERE` window-function filter, e.g. `QUALIFY ROW_NUMBER() OVER (...) = 1`)

The project `.vscode/settings.json` associates `**/databricks/**/*.sql` with the generic `sql` language to reduce but not eliminate these diagnostics.
