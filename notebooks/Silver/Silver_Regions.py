# Databricks notebook source
# ==============================================================================
# Silver Layer - Region Data Transformation
# ==============================================================================
# Purpose: Transforms region data from Bronze layer into Delta table format.
#          Performs minimal transformations (removes rescued columns) as
#          region is a small dimension table with limited data quality issues.
#
# Key Operations:
#   - Read region data from Bronze catalog table
#   - Remove rescued data columns
#   - Materialize as Delta table for consistency with medallion architecture
#
# Output: Delta table at silver.regions_silver
# ==============================================================================

# Read region dimension table from Bronze layer
df = spark.table("databricks_catalog.bronze.regions")
df.show(10)

# COMMAND ----------

# Remove _rescued_data column (contains columns that failed schema validation)
df = df.drop("_rescued_data")

# COMMAND ----------

# Write transformed data as Delta table
df.write.format("delta")\
        .mode("overwrite")\
        .save("abfss://silver@databrickseteloua.dfs.core.windows.net/regions")

# COMMAND ----------

df = spark.read.format("delta")\
                .load("abfss://silver@databrickseteloua.dfs.core.windows.net/products")

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickseteloua.dfs.core.windows.net/regions'
