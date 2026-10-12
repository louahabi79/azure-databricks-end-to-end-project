# Databricks notebook source
# ==============================================================================
# Gold Layer - Order Facts (FactOrders) - Star Schema
# ==============================================================================
# Purpose: Builds an order fact table using star schema design pattern.
#          Joins order transactions with customer and product dimensions
#          to create a denormalized analytics table suitable for OLAP queries.
#
# Key Operations:
#   1. Read order transactions from Silver layer
#   2. Join with DimCustomers to retrieve DimCustomerKey (surrogate key)
#   3. Join with DimProducts to retrieve DimProductKey (surrogate key)
#   4. Replace natural keys with surrogate keys for referential integrity
#   5. Upsert data using Delta merge operation for idempotent loads
#
# Star Schema:
#   - Fact table (FactOrders): contains transaction measures and dimension keys
#   - Dimension tables (DimCustomers, DimProducts): contain descriptive attributes
#   - Allows efficient aggregation queries and supports dimensional analysis
#
# Output: Delta table at gold.FactOrders (Fact table)
# ==============================================================================

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

# Remove natural keys (customer_id, product_id) as they are replaced by surrogate keys
# Fact table now contains only dimension keys for referential integrity
df_fact_new = _sqldf.drop("customer_id", "product_id")
df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **upsert on fact table**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Merge operation: updates changed facts and inserts new ones
# Match condition: order_id + dimension keys uniquely identify a fact row
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
