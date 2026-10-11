# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet")\
                .load("abfss://bronze@databrickseteloua.dfs.core.windows.net/customers")

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

df.show(10)

# COMMAND ----------

df = df.withColumn("domain", split(col("email"), "@")[1])
df.show(10)

# COMMAND ----------

df.createOrReplaceTempView("customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     domain,
# MAGIC     COUNT(*) AS users
# MAGIC FROM customers
# MAGIC GROUP BY domain
# MAGIC ORDER BY users DESC

# COMMAND ----------

df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
df = df.drop("first_name", "last_name")
df.show(10)

# COMMAND ----------

columns_order = ["customer_id", "full_name", "state", "city", "email", "domain"]
df = df.select(columns_order)
df.show(5)

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .save("abfss://silver@databrickseteloua.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickseteloua.dfs.core.windows.net/customers'