# Databricks notebook source
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

df = df.drop("_rescued_data")

# COMMAND ----------

df.show(10)

# COMMAND ----------

df = df.withColumn("year", year(col("order_date")))
df.show(10)

# COMMAND ----------

columns_order = ["order_id", "customer_id", "product_id", "quantity", "total_amount", "year", "order_date"]
df = df.select(columns_order)
df.show(10)

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .save("abfss://silver@databrickseteloua.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickseteloua.dfs.core.windows.net/orders'