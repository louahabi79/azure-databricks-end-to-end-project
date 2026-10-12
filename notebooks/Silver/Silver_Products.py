# Databricks notebook source
# ==============================================================================
# Silver Layer - Product Data Transformation
# ==============================================================================
# Purpose: Transforms raw product data from Bronze layer into clean, structured 
#          format. Applies data quality rules, creates reusable SQL functions,
#          derives calculated fields, and materializes as Delta tables.
#
# Key Transformations:
#   - Clean and trim product attributes (name, category, brand)
#   - Create discount_func for consistent pricing calculations
#   - Calculate discounted_price based on 10% discount rule
#   - Store as Delta table for ACID compliance
#
# Output: Delta table at silver.products_silver
# ==============================================================================

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("parquet")\
                .load("abfss://bronze@databrickseteloua.dfs.core.windows.net/products")

# COMMAND ----------

df.display()

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     TRIM(product_name) AS product_name,
# MAGIC     TRIM(category) AS category,
# MAGIC     TRIM(brand) AS brand,
# MAGIC     price
# MAGIC FROM products

# COMMAND ----------

_sqldf.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_catalog.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *,
# MAGIC     databricks_catalog.bronze.discount_func(price) AS discounted_price
# MAGIC FROM products

# COMMAND ----------

# Apply discount calculation using the defined function
df.withColumn("discounted_price", expr("databricks_catalog.bronze.discount_func(price)")).show(10)

# COMMAND ----------

df = _sqldf

# COMMAND ----------

# Write transformed data as Delta table with schema evolution support
df.write.format("delta")\
        .mode("overwrite")\
        .option("mergeSchema", True)\
        .save("abfss://silver@databrickseteloua.dfs.core.windows.net/products")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickseteloua.dfs.core.windows.net/products'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_catalog.silver.products_silver
