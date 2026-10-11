# Databricks notebook source
df = spark.table("databricks_catalog.bronze.regions")
df.show(10)

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

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