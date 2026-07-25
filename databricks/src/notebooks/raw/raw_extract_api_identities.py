# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC ## Raw Extract — API Identities (Users & Service Principals)
# MAGIC
# MAGIC Extracts users and service principals from the Databricks SCIM REST API and writes
# MAGIC two Parquet files to a Unity Catalog Volume — one per resource type.
# MAGIC
# MAGIC | Item | Value |
# MAGIC |---|---|
# MAGIC | Source | Databricks SCIM API (account-level preferred, workspace fallback) |
# MAGIC | Output — users | `{volume_base_path}/raw/api_users/{workspace_name}/{yyyy-mm-dd-hhmmss}-{workspace_name}-api_users.parquet` |
# MAGIC | Output — SPs | `{volume_base_path}/raw/api_service_principals/{workspace_name}/{yyyy-mm-dd-hhmmss}-{workspace_name}-api_service_principals.parquet` |
# MAGIC | Strategy | Full snapshot per run, new timestamped file — SCIM is the authoritative identity source |
# MAGIC | Retention | Extracts older than `retention_days` (default 7) are deleted at the end of each run |
# MAGIC
# MAGIC **Why a new file per run (not overwrite):** the SAL Auto Loader tracks ingested files by
# MAGIC *path* in its checkpoint — an overwritten file with the same name is considered already
# MAGIC processed and silently skipped after the first load. File names must be unique per run.
# MAGIC
# MAGIC **Endpoint selection:** account-level SCIM (`/accounts/{account_id}/scim/v2`) is probed
# MAGIC first; falls back to workspace-level SCIM if the account endpoint is unavailable.

# COMMAND ----------

# DBTITLE 1,Setup — widgets & SCIM endpoint
import requests
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

dbutils.widgets.text("catalog_env",      "dev")
dbutils.widgets.text("catalog_name",     "it_security")
dbutils.widgets.text("schema",           "databricks")
dbutils.widgets.text("folder_base_path", "system_tables_landing")
dbutils.widgets.text("account_id",       "9f336835-f3ce-4a17-b925-e2e1b67d8ffd")
dbutils.widgets.text("retention_days",   "7")

catalog_env      = dbutils.widgets.get("catalog_env")
catalog_name     = dbutils.widgets.get("catalog_name")
schema           = dbutils.widgets.get("schema")
folder_base_path = dbutils.widgets.get("folder_base_path")
account_id       = dbutils.widgets.get("account_id")
retention_days   = int(dbutils.widgets.get("retention_days"))

# Unique name per run — Auto Loader never reprocesses an already-seen path, so each
# extract must land as a new file (same pattern as raw_extract_system_tables.py).
run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")

catalog          = f"{catalog_name}_{catalog_env}"
volume_base_path = f"/Volumes/{catalog}/{schema}/{folder_base_path}"

ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host  = ctx.apiUrl().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}"}

# apiUrl is populated in every execution context (interactive, jobs, serverless),
# unlike browserHostName which only exists in interactive sessions.
workspace_url  = host.replace("https://", "")
workspace_name = workspace_url.split(".")[0]

if "azuredatabricks.net" in host:
    account_host = "https://accounts.azuredatabricks.net"
elif "gcp.databricks.com" in host:
    account_host = "https://accounts.gcp.databricks.com"
else:
    account_host = "https://accounts.cloud.databricks.com"

account_scim_base   = f"{account_host}/api/2.0/accounts/{account_id}/scim/v2"
workspace_scim_base = f"{host}/api/2.0/preview/scim/v2"

try:
    _probe = requests.get(
        f"{account_scim_base}/Users",
        headers=headers,
        params={"startIndex": 1, "count": 1},
    )
    _probe.raise_for_status()
    active_scim_base = account_scim_base
    print(f"✓ SCIM endpoint : account-level ({account_scim_base})")
except Exception as _e:
    print(f"⚠ Account-level SCIM failed ({_e}), trying workspace-level ...")
    try:
        _probe = requests.get(
            f"{workspace_scim_base}/Users",
            headers=headers,
            params={"startIndex": 1, "count": 1},
        )
        _probe.raise_for_status()
        active_scim_base = workspace_scim_base
        print(f"✓ SCIM endpoint : workspace-level ({workspace_scim_base})")
    except Exception as _e2:
        raise RuntimeError(
            f"Both SCIM endpoints failed — aborting notebook.\n"
            f"  Account-level  : {account_scim_base}\n"
            f"  Workspace-level: {workspace_scim_base}\n"
            f"  Last error     : {_e2}"
        ) from _e2

print(f"Catalog      : {catalog}")
print(f"Workspace    : {workspace_name}")
print(f"Volume path  : {volume_base_path}")
print(f"Account ID   : {account_id}")

# COMMAND ----------

# DBTITLE 1,Extract users from SCIM API → Parquet
_PAGE_SIZE = 100
_start     = 1
_users     = []
_load_dt   = datetime.now(timezone.utc)

while True:
    r = requests.get(
        f"{active_scim_base}/Users",
        headers=headers,
        params={"startIndex": _start, "count": _PAGE_SIZE},
    )
    r.raise_for_status()
    data      = r.json()
    resources = data.get("Resources", [])
    if not resources:
        break
    for u in resources:
        primary_email = next(
            (e["value"] for e in u.get("emails", []) if e.get("primary")),
            None,
        )
        _users.append((
            str(u.get("id", "")),
            account_id,
            u.get("userName"),
            u.get("displayName"),
            u.get("name", {}).get("givenName"),
            u.get("name", {}).get("familyName"),
            1 if u.get("active", False) else 0,
            u.get("externalId"),
            primary_email,
            _load_dt,
        ))
    _total  = data.get("totalResults", 0)
    _start += _PAGE_SIZE
    if _start > _total:
        break

print(f"Users fetched: {len(_users)}")

if _users:
    _schema_users = StructType([
        StructField("user_id_cd",       StringType(),    True),
        StructField("account_id",       StringType(),    True),
        StructField("username_lb",      StringType(),    True),
        StructField("display_name_lb",  StringType(),    True),
        StructField("first_name_lb",    StringType(),    True),
        StructField("last_name_lb",     StringType(),    True),
        StructField("is_active_fl",     IntegerType(),   True),
        StructField("external_id_cd",   StringType(),    True),
        StructField("primary_email_lb", StringType(),    True),
        StructField("sys_load_dt",      TimestampType(), True),
    ])

    # Workspace subfolder — same layout as raw_extract_system_tables.py; the SAL Auto Loader
    # reads the parent folder and discovers workspace subfolders recursively.
    out_dir    = f"{volume_base_path}/raw/api_users/{workspace_name}"
    final_path = f"{out_dir}/{run_ts}-{workspace_name}-api_users.parquet"
    temp_path  = f"{out_dir}/_tmp"
    dbutils.fs.mkdirs(out_dir)

    spark.createDataFrame(_users, schema=_schema_users) \
         .coalesce(1).write.mode("overwrite").parquet(temp_path)

    part_file = next(f.path for f in dbutils.fs.ls(temp_path) if f.name.endswith(".parquet"))
    dbutils.fs.mv(part_file, final_path)
    dbutils.fs.rm(temp_path, True)

    print(f"✓ Written {len(_users)} rows → {final_path}")
else:
    print("⚠ Users: API returned no records — file not written")

# COMMAND ----------

# DBTITLE 1,Extract service principals from SCIM API → Parquet
_PAGE_SIZE = 100
_start     = 1
_sps       = []
_load_dt   = datetime.now(timezone.utc)

while True:
    r = requests.get(
        f"{active_scim_base}/ServicePrincipals",
        headers=headers,
        params={"startIndex": _start, "count": _PAGE_SIZE},
    )
    r.raise_for_status()
    data      = r.json()
    resources = data.get("Resources", [])
    if not resources:
        break
    for sp in resources:
        _sps.append((
            str(sp.get("id", "")),
            account_id,
            str(sp.get("applicationId", "")),
            sp.get("displayName"),
            1 if sp.get("active", False) else 0,
            sp.get("externalId"),
            _load_dt,
        ))
    _total  = data.get("totalResults", 0)
    _start += _PAGE_SIZE
    if _start > _total:
        break

print(f"Service principals fetched: {len(_sps)}")

if _sps:
    _schema_sps = StructType([
        StructField("sp_id_cd",          StringType(),    True),
        StructField("account_id",        StringType(),    True),
        StructField("application_id_cd", StringType(),    True),
        StructField("display_name_lb",   StringType(),    True),
        StructField("is_active_fl",      IntegerType(),   True),
        StructField("external_id_cd",    StringType(),    True),
        StructField("sys_load_dt",       TimestampType(), True),
    ])

    out_dir    = f"{volume_base_path}/raw/api_service_principals/{workspace_name}"
    final_path = f"{out_dir}/{run_ts}-{workspace_name}-api_service_principals.parquet"
    temp_path  = f"{out_dir}/_tmp"
    dbutils.fs.mkdirs(out_dir)

    spark.createDataFrame(_sps, schema=_schema_sps) \
         .coalesce(1).write.mode("overwrite").parquet(temp_path)

    part_file = next(f.path for f in dbutils.fs.ls(temp_path) if f.name.endswith(".parquet"))
    dbutils.fs.mv(part_file, final_path)
    dbutils.fs.rm(temp_path, True)

    print(f"✓ Written {len(_sps)} rows → {final_path}")
else:
    print("⚠ Service principals: API returned no records — file not written")

# COMMAND ----------

# DBTITLE 1,Prune expired extracts
# Storage hygiene only — Auto Loader has already checkpointed processed files, so deleting
# old extracts never causes re-ingestion. Keeps directory listing fast as runs accumulate.
import time

cutoff_ms = int(time.time() * 1000) - retention_days * 86400 * 1000
for _dir in (
    f"{volume_base_path}/raw/api_users/{workspace_name}",
    f"{volume_base_path}/raw/api_service_principals/{workspace_name}",
):
    try:
        entries = dbutils.fs.ls(_dir)
    except Exception:
        continue  # directory not created yet (e.g. empty API response on every run so far)
    for f in entries:
        if f.name.endswith(".parquet") and f.modificationTime < cutoff_ms:
            dbutils.fs.rm(f.path)
            print(f"Deleted expired extract: {f.name}")

# COMMAND ----------

# DBTITLE 1,Summary
print("✅ Raw extract complete!")
print(f"  Workspace    : {workspace_name}")
print(f"  Account ID   : {account_id}")
print(f"  Users        : {len(_users)} rows")
print(f"  Service PCs  : {len(_sps)} rows")
