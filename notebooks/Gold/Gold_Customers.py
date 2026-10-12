# Databricks notebook source
# ==============================================================================
# Gold Layer - Customer Dimension (DimCustomers) - SCD Type 2
# ==============================================================================
# Purpose: Builds a customer dimension table using Slowly Changing Dimensions 
#          (SCD Type 2) methodology. Tracks customer attribute changes over time
#          by maintaining a history of all versions with effective date ranges.
#
# Key Operations:
#   1. Extract new and existing customers
#   2. Generate surrogate keys for dimensional integrity
#   3. Separate customers into new and changed records
#   4. Apply SCD Type 2 logic: update old records with end dates, insert new versions
#   5. Merge with existing dimension table using Delta Lake merge operation
#
# SCD Type 2 Details:
#   - All records (old and new) are maintained in the table
#   - create_date: When the customer first appeared (unchanged for versions of same customer)
#   - update_date: When the record was last modified
#   - DimCustomerKey: Surrogate key (unique per customer version)
#
# Output: Delta table at gold.DimCustomers (Dimension table)
# ==============================================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading From Source**

# COMMAND ----------

df = spark.table("databricks_catalog.silver.customers_silver")
df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Drop Duplicates**

# COMMAND ----------

# Ensure only one record per customer (handles duplicate ingestions)
df = df.dropDuplicates(["customer_id"])
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Devinding New vs Old Records**

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_catalog

# COMMAND ----------

# Parameter to distinguish initial load (1) from incremental loads (0)
# Initial load creates dimension from scratch; incremental loads preserve history
init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# For incremental loads: retrieve only customer keys and dates from existing dimension
# For initial load: create empty dataframe with matching schema
if init_load_flag == 0:
    df_old = spark.sql('''SELECT 
                                DimCustomerKey,
                                customer_id,
                                create_date,
                                update_date
                          FROM gold.DimCustomers''')
else:
    df_old = spark.sql('''
                        SELECT
                            0 AS DimCustomerKey,
                            0 AS customer_id,
                            0 AS create_date,
                            0 AS update_date
                        FROM silver.customers_silver
                        WHERE 1=0
                       ''')

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Rnename columns**

# COMMAND ----------

# Prefix old columns to distinguish from new data during join
df_old = df_old.withColumnsRenamed({"DimCustomerKey":"old_DimCustomerKey", "customer_id":"old_customer_id", "create_date":"old_create_date", "update_date":"old_update_date"})

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Applying Join With The Old Records**

# COMMAND ----------

# Left join ensures all new records are included; old records matched by customer_id
df_join = df.join(df_old, df.customer_id == df_old.old_customer_id, "left")

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Separating New vs Old Records**

# COMMAND ----------

# New customers: no match in old dimension (old_DimCustomerKey is NULL)
df_new = df_join.filter(col("old_DimCustomerKey").isNull())

# COMMAND ----------

# Existing customers: matched in old dimension (old_DimCustomerKey is NOT NULL)
df_old = df_join.filter(col("old_DimCustomerKey").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing df_old**

# COMMAND ----------

# Dropping all not required columns
df_old = df_old.drop("old_customer_id", "old_update_date")

df_old = df_old.withColumnRenamed("old_DimCustomerKey", "DimCustomerKey")

# Renaming "old_create_date" to "create_date"
df_old = df_old.withColumnRenamed("old_create_date", "create_date")

# adding update_date
df_old = df_old.withColumn("update_date", current_timestamp())


# COMMAND ----------

df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))

# COMMAND ----------

df_old.display()

# COMMAND ----------

#Dropping all not required columns
df_new = df_new.drop("old_DimCustomerKey", "old_customer_id", "old_update_date", "old_create_date")

# Adding create_date & update_date
df_new = df_new.withColumn("create_date", current_timestamp())
df_new = df_new.withColumn("update_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Surrogate Key - All The Values**

# COMMAND ----------

# Generate surrogate keys: initial load starts at 1, incremental loads increment from max
if init_load_flag == 1:
        max_surrogate_Key = 0
else:
        df_maxSur = spark.sql("SELECT MAX(DimCustomerKey) AS maxSur FROM gold.DimCustomers")
        max_surrogate_Key = df_maxSur.first()["maxSur"]

# COMMAND ----------

# Assign unique surrogate keys to new records (monotonically_increasing_id ensures uniqueness)
df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id()+lit(max_surrogate_Key+1))

# COMMAND ----------

# Combine updated old records (SCD Type 2 history) with new records
df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Merge operation: updates existing records and inserts new ones (upsert pattern)
if spark.catalog.tableExists("databricks_catalog.gold.DimCustomers"):
    dlt_obj = DeltaTable.forPath(spark, "abfss://gold@databrickseteloua.dfs.core.windows.net/DimCustomers")
    dlt_obj.alias("trg").merge(df_final.alias("src"), "trg.DimCustomerKey == src.DimCustomerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_final.write.format("delta")\
                    .mode("overwrite")\
                    .option("path", "abfss://gold@databrickseteloua.dfs.core.windows.net/DimCustomers")\
                    .saveAsTable("databricks_catalog.gold.DimCustomers")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dimcustomers
