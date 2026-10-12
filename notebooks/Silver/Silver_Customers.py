# Databricks notebook source
# ==============================================================================
# Silver Layer - Customer Data Transformation
# ==============================================================================
# Purpose: Transforms raw customer data from Bronze layer into clean, 
#          structured format for analytics. Applies data quality rules,
#          text normalization, feature engineering, and materialization 
#          as Delta tables.
#
# Key Transformations:
#   - Remove rescued data columns (schema mismatch indicators)
#   - Trim whitespace from string columns
#   - Extract email domain for segmentation analysis
#   - Concatenate first/last names into single full_name field
#   - Reorder columns for standardized schema
#   - Store as Delta table for ACID compliance and time travel
#
# Output: Delta table at silver.customers_silver
# ==============================================================================

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

# Remove _rescued_data column (contains columns that failed schema validation)
df = df.drop("_rescued_data")

# COMMAND ----------

# Trim leading/trailing whitespace from all string columns to ensure data quality
for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# COMMAND ----------

df.show(10)

# COMMAND ----------

# Extract email domain (text after @) for customer segmentation analysis
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

# Combine first and last names into a single field and remove source columns
df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
df = df.drop("first_name", "last_name")
df.show(10)

# COMMAND ----------

# Reorder columns in standardized sequence for consistency
columns_order = ["customer_id", "full_name", "state", "city", "email", "domain"]
df = df.select(columns_order)
df.show(5)

# COMMAND ----------

# Write transformed data as Delta table for ACID compliance
df.write.format("delta")\
        .mode("overwrite")\
        .save("abfss://silver@databrickseteloua.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.customers_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databrickseteloua.dfs.core.windows.net/customers'
