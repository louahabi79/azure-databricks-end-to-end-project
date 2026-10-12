# Databricks notebook source
# ==============================================================================
# Gold Layer - Product Dimension (DimProducts) - SCD Type 2 with Delta Live Tables
# ==============================================================================
# Purpose: Builds a product dimension table using Delta Live Tables (DLT) and 
#          Slowly Changing Dimensions (SCD Type 2) methodology. Tracks product
#          attribute changes over time while automatically validating data quality.
#
# Key Components:
#   1. DimProducts_stage: Streaming table with data quality expectations
#      - Validates: product_id IS NOT NULL, product_name IS NOT NULL
#      - Drops invalid records (violating expectations)
#   2. DimProducts_view: Streaming view for data pipeline composition
#   3. DimProducts: Main dimension table with SCD Type 2 tracking
#      - Tracks changes to product attributes over time
#      - Maintains history with effective date ranges
#      - Uses product_id as the key for change detection
#
# SCD Type 2 Details:
#   - stored_as_scd_type = 2: Maintains full history of changes
#   - sequence_by = product_id: Orders records for change detection
#   - All versions of a product remain in the table
#
# Output: Streaming tables (DimProducts_stage, DimProducts)
#         Managed by Delta Live Tables framework
# ==============================================================================

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming Table**

# COMMAND ----------

# Define data quality expectations for product records
# rule1: Enforces product_id is not null (unique identifier)
# rule2: Enforces product_name is not null (required attribute)
my_rules = {
    "rule1": "product_id IS NOT NULL",
    "rule2": "product_name IS NOT NULL"
}

# COMMAND ----------

# Create streaming table with quality expectations
# @dlt.expect_all_or_drop: Records violating any rule are dropped (not stored)
# This ensures only valid products reach downstream tables
@dlt.table()
@dlt.expect_all_or_drop(my_rules)
def DimProducts_stage():
    df = spark.readStream.table("databricks_catalog.silver.products_silver")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming View**

# COMMAND ----------

# Create streaming view for pipeline composition
# Views allow transformation logic reuse without storing intermediate data
@dlt.view()
def DimProducts_view():
    df = spark.readStream.table("Live.DimProducts_stage")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC **DimProducts**

# COMMAND ----------

# Create streaming table for dimension
# This table will hold both current and historical product versions
dlt.create_streaming_table("DimProducts")

# COMMAND ----------

# Apply SCD Type 2 transformation: maintain full history of product changes
# - keys: Uniquely identifies a product (natural key)
# - sequence_by: Column order (determines which version is latest when out-of-order data arrives)
# - stored_as_scd_type: 2 = maintain complete history; 1 = overwrite
dlt.apply_changes(
    source = "Live.DimProducts_view",
    target = "DimProducts",
    keys = ["product_id"],                  # Primary keys used to match rows
    sequence_by = "product_id",        # Column to handle out-of-order records
    stored_as_scd_type = 2,                  # 1 for overwrite, 2 for history tracking
)
