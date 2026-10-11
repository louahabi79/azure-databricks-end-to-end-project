# Databricks notebook source
# MAGIC %md
# MAGIC ### **Fact Orders**

# COMMAND ----------

# MAGIC %md
# MAGIC **Data Reading**

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_catalog

# COMMAND ----------

df = spark.sql("SELECT * FROM silver.orders_silver")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Fact Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     so.*,
# MAGIC     gc.DimCustomerKey,
# MAGIC     gp.product_id AS DimProductKey
# MAGIC FROM SILVER.orders_silver so
# MAGIC LEFT JOIN gold.dimcustomers gc
# MAGIC ON so.customer_id = gc.customer_id
# MAGIC LEFT JOIN gold.dimproducts gp
# MAGIC ON so.product_id = gp.product_id

# COMMAND ----------

df_fact_new = _sqldf.drop("customer_id", "product_id")
df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **upsert on fact table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_catalog.gold.factorders"):
    dlt_obj = DeltaTable.forName(spark, "databricks_catalog.gold.factorders")
    dlt_obj.alias("trg").merge(df_fact_new.alias("src"), "trg.order_id = src.order_id AND trg.DimCustomerKey = src.DimCustomerKey AND trg.DimProductKey = src.DimProductKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact_new.write.format("delta")\
                        .option("path", "abfss://gold@databrickseteloua.dfs.core.windows.net/FactOrders")\
                        .saveAsTable("databricks_catalog.gold.factorders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.factorders