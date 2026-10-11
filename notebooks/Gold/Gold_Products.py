# Databricks notebook source
# MAGIC %md
# MAGIC ### **DLT Pipeline**

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming Table**

# COMMAND ----------

my_rules = {
    "rule1": "product_id IS NOT NULL",
    "rule2": "product_name IS NOT NULL"
}

# COMMAND ----------

@dlt.table()

@dlt.expect_all_or_drop(my_rules)
def DimProducts_stage():
    df = spark.readStream.table("databricks_catalog.silver.products_silver")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming View**

# COMMAND ----------

@dlt.view()
def DimProducts_view():
    df = spark.readStream.table("Live.DimProducts_stage")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC **DimProducts**

# COMMAND ----------

dlt.create_streaming_table("DimProducts")

# COMMAND ----------

dlt.apply_changes(
    source = "Live.DimProducts_view",
    target = "DimProducts",
    keys = ["product_id"],                  # Primary keys used to match rows
    sequence_by = "product_id",        # Column to handle out-of-order records
    stored_as_scd_type = 2,                  # 1 for overwrite, 2 for history tracking
)