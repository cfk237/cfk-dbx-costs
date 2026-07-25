# Databricks notebook source
# DBTITLE 1,Notebook Summary
# MAGIC %md
# MAGIC ## DLT SAL — List Prices
# MAGIC
# MAGIC Auto Loader ingestion of `system.billing.list_prices` Parquet (produced by
# MAGIC `raw_extract_list_prices.py`) into `sal_billing_list_prices`.
# MAGIC
# MAGIC **Tier 1** — `system.billing.list_prices` is a global table; files land under
# MAGIC `{volume_base_path}/raw/system_billing_list_prices/{workspace_name}/` (one timestamped
# MAGIC file per extract run). Auto Loader reads the parent folder and discovers workspace
# MAGIC subfolders recursively.

# COMMAND ----------

from pyspark import pipelines as dp

_bronze = {"quality": "bronze"}

catalog_name = spark.conf.get("pipeline.catalog_name", "it_security")
catalog_env  = spark.conf.get("pipeline.catalog_env",  "dev")
volume_name  = spark.conf.get("pipeline.volume_name",  "system_tables_landing")

catalog          = f"{catalog_name}_{catalog_env}"
bronze_schema        = spark.conf.get("pipeline.schema_bronze", "databricks")
volume_base_path = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}"

# COMMAND ----------

# DBTITLE 1,Billing list prices
dp.create_streaming_table("sal_billing_list_prices", table_properties=_bronze,
    comment="SAL staging for system.billing.list_prices. Full snapshot per load run.")

@dp.append_flow(target="sal_billing_list_prices", name="f_sal_billing_list_prices")
def f_sal_billing_list_prices():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",              "parquet")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes",    "true")
        .load(f"{volume_base_path}/raw/system_billing_list_prices")
        .selectExpr(
            "*",
            "_metadata.file_name               AS _source_file_name",
            "_metadata.file_modification_time  AS _source_file_dt",
            "current_timestamp()               AS sal_load_dt",
        )
    )
