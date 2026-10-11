# Databricks notebook source
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

df = df.dropDuplicates(["customer_id"])
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Devinding New vs Old Records**

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_catalog

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

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

df_old = df_old.withColumnsRenamed({"DimCustomerKey":"old_DimCustomerKey", "customer_id":"old_customer_id", "create_date":"old_create_date", "update_date":"old_update_date"})

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Applying Join With The Old Records**

# COMMAND ----------

df_join = df.join(df_old, df.customer_id == df_old.old_customer_id, "left")

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Separating New vs Old Records**

# COMMAND ----------

df_new = df_join.filter(col("old_DimCustomerKey").isNull())

# COMMAND ----------

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

if init_load_flag == 1:
        max_surrogate_Key = 0
else:
        df_maxSur = spark.sql("SELECT MAX(DimCustomerKey) AS maxSur FROM gold.DimCustomers")
        max_surrogate_Key = df_maxSur.first()["maxSur"]

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id()+lit(max_surrogate_Key+1))

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

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