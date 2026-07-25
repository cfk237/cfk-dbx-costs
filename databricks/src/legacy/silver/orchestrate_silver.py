# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC ## Silver Orchestration
# MAGIC
# MAGIC Runs all raw extract, SAL ingest, and silver notebooks sequentially in dependency order.
# MAGIC Each sub-notebook is called via `dbutils.notebook.run()` with the appropriate parameters.
# MAGIC
# MAGIC | Step | Notebook | Depends on |
# MAGIC |---|---|---|
# MAGIC | 1 | `raw_extract_system_tables` | — |
# MAGIC | 2 | `raw_extract_list_prices` | — |
# MAGIC | 3 | `raw_extract_workspaces` | — |
# MAGIC | 4 | `raw_extract_api_identities` | — |
# MAGIC | 5 | `sal_ingest_system_tables` | step 1 |
# MAGIC | 6 | `sal_ingest_list_prices` | step 2 |
# MAGIC | 7 | `sal_ingest_workspaces` | step 3 |
# MAGIC | 8 | `sal_ingest_api_identities` | step 4 |
# MAGIC | 9 | `silver_lakeflow_jobs` | step 5 |
# MAGIC | 10 | `silver_lakeflow_pipelines` | step 5 |
# MAGIC | 11 | `silver_compute_clusters` | step 5 |
# MAGIC | 12 | `silver_compute_warehouses` | step 5 |
# MAGIC | 13 | `silver_billing_usage` | step 5 |
# MAGIC | 14 | `silver_billing_list_prices` | step 6 |
# MAGIC | 15 | `silver_billing_cost` | steps 13, 14 |
# MAGIC | 16 | `silver_workspace` | step 7 |
# MAGIC | 17 | `silver_identity` | step 8 |
# MAGIC | 18 | `silver_resource` | steps 9–13 |
# MAGIC | 19 | `silver_tag_key` | steps 9–12 |
# MAGIC | 20 | `silver_resource_tag` | steps 18, 19 |
# MAGIC
# MAGIC **Error handling**: fail-fast — any notebook failure raises immediately with the notebook name.
# MAGIC All sub-notebooks are idempotent (watermarks, MERGE) so the whole run can be retried safely.
# MAGIC
# MAGIC **`include_billing_usage`**: set `true` on the single central workspace that collects billing data.
# MAGIC **`reset_checkpoints`**: set `true` only to force a full re-extract + re-ingest (clears checkpoints and SAL tables).

# COMMAND ----------

# DBTITLE 1,Widgets & parameter dicts
dbutils.widgets.text("catalog_env",   "dev")
dbutils.widgets.text("catalog_name",  "it_security")
dbutils.widgets.text("account_id",    "9f336835-f3ce-4a17-b925-e2e1b67d8ffd")
dbutils.widgets.dropdown("include_billing_usage", "false", ["false", "true"])
dbutils.widgets.dropdown("reset_checkpoints",     "false", ["false", "true"])

catalog_env           = dbutils.widgets.get("catalog_env")
catalog_name          = dbutils.widgets.get("catalog_name")
account_id            = dbutils.widgets.get("account_id")
include_billing_usage = dbutils.widgets.get("include_billing_usage")
reset_checkpoints     = dbutils.widgets.get("reset_checkpoints")

# Base params passed to every notebook
shared = {"catalog_env": catalog_env, "catalog_name": catalog_name}

# Extended param sets
raw      = {**shared, "include_billing_usage": include_billing_usage, "reset_checkpoints": reset_checkpoints}
identity = {**shared, "account_id": account_id}

print(f"catalog               : {catalog_name}_{catalog_env}")
print(f"account_id            : {account_id or '(empty — workspace-level SCIM fallback)'}")
print(f"include_billing_usage : {include_billing_usage}")
print(f"reset_checkpoints     : {reset_checkpoints}")

# COMMAND ----------

# DBTITLE 1,Runner helper
import time

def run(name, timeout_seconds, params=None):
    """Run a sub-notebook and fail-fast on error."""
    print(f"\n▶  {name}")
    t0 = time.time()
    try:
        dbutils.notebook.run(f"./{name}", timeout_seconds, params or shared)
        elapsed = round(time.time() - t0)
        print(f"   ✓ {name}  ({elapsed}s)")
    except Exception as e:
        elapsed = round(time.time() - t0)
        raise RuntimeError(
            f"Notebook '{name}' failed after {elapsed}s.\n  Error: {e}"
        ) from e

# COMMAND ----------

# DBTITLE 1,Steps 1–4 — Raw extract (Parquet landing)
# Step 1: streaming system tables (jobs, pipelines, clusters, warehouses, billing.usage).
# Step 2: system.billing.list_prices (full refresh, no streaming).
# Step 3: system.access.workspaces_latest (full refresh, no streaming).
# Step 4: SCIM API users & service principals (full refresh, no streaming).
run("../../notebooks/raw/raw_extract_system_tables", 3600, raw)
run("../../notebooks/raw/raw_extract_list_prices",   1800)
run("../../notebooks/raw/raw_extract_workspaces",     600)
run("../../notebooks/raw/raw_extract_api_identities", 600, identity)

# COMMAND ----------

# DBTITLE 1,Steps 5–8 — SAL ingest (Parquet → Delta)
# Step 5: Auto Loader — all workspace system table Parquet files → SAL Delta tables.
# Step 6: Auto Loader — list_prices Parquet → sal_billing_list_prices.
# Step 7: Auto Loader — workspace Parquet → sal_workspaces.
# Step 8: Auto Loader — identity Parquet → sal_users + sal_service_principals.
run("../sal/sal_ingest_system_tables",  3600, raw)
run("../sal/sal_ingest_list_prices",    1800)
run("../sal/sal_ingest_workspaces",      600)
run("../sal/sal_ingest_api_identities", 600)

# COMMAND ----------

# DBTITLE 1,Steps 9–12 — SCD2 silver dimensions
run("./silver_lakeflow_jobs",      1800)
run("./silver_lakeflow_pipelines", 1800)
run("./silver_compute_clusters",   1800)
run("./silver_compute_warehouses", 1800)

# COMMAND ----------

# DBTITLE 1,Step 13 — Billing usage fact
run("./silver_billing_usage", 3600)

# COMMAND ----------

# DBTITLE 1,Step 14 — Billing list prices dimension
run("./silver_billing_list_prices", 600)

# COMMAND ----------

# DBTITLE 1,Step 15 — Billing cost fact (requires steps 13 and 14)
run("../../notebooks/silver/silver_billing_cost", 1800)

# COMMAND ----------

# DBTITLE 1,Step 16 — Workspace dimension
run("./silver_workspace", 300)

# COMMAND ----------

# DBTITLE 1,Step 17 — Identity silver (users, service principals, identity)
run("./silver_identity", 600)

# COMMAND ----------

# DBTITLE 1,Steps 18–20 — Enrichment dimensions & bridge
run("./silver_resource",     600)
run("../../notebooks/silver/silver_tag_key",      300)
run("../../notebooks/silver/silver_resource_tag", 600)

# COMMAND ----------

# DBTITLE 1,Summary
print("\n✅ All notebooks completed successfully.")
print(f"   catalog               : {catalog_name}_{catalog_env}")
print(f"   include_billing_usage : {include_billing_usage}")
print(f"   reset_checkpoints     : {reset_checkpoints}")
