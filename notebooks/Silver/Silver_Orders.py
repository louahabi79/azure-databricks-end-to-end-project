# Databricks notebook source
# ==============================================================================
# Silver Layer - Order Data Transformation
# ==============================================================================
# Purpose: Transforms raw order data from Bronze layer into clean, structured 
#          format for analytics. Applies data quality rules, feature engineering
#          (year extraction), and materialization as Delta tables.
#
# Key Transformations:
#   - Remove rescued data columns (schema mismatch indicators)
#   - Extract year from order_date for time-based partitioning and analysis
#   - Reorder columns for standardized schema
#   - Store as Delta table for ACID compliance and time travel
#
# Output: Delta table at silver.orders_silver
# ==============================================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
                .load("abfss://bronze@databrickseteloua.dfs.core.windows.net/orders/*.parquet")

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Remove _rescued_data column (contains columns that failed schema validation)
df = df.drop("_rescued_data")

# COMMAND ----------

df.show(10)

# COMMAND ----------

# Extract year from order_date for time-based analysis and partitioning
df = df.withColumn("year", year(col("order_date")))
df.show(10)

# COMMAND ----------

# Reorder columns in standardized sequence for consistency
columns_order = ["order_id", "customer_id", "product_id", "quantity", "total_amount", "year", "order_date"]
df = df.select(columns_order)
df.show(10)

# COMMAND ----------

# Write transformed data as Delta table for ACID compliance
df.write.format("delta")\
        .mode("overwrite")\
        .save("abfss://silver@databrickseteloua.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickseteloua.dfs.core.windows.net/orders'
