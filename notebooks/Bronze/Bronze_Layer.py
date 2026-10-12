# Databricks notebook source
# ==============================================================================
# Bronze Layer - Data Ingestion Pipeline
# ==============================================================================
# Purpose: Ingests raw data files from cloud storage using Auto Loader and 
#          streams them into the Bronze layer as Parquet files with schema 
#          evolution and checkpoint management.
#
# Key Features:
#   - Auto Loader for automatic schema detection and evolution
#   - Streaming ingestion with fault tolerance
#   - Checkpoint-based recovery for reliable data processing
#   - Parametrized file names for dynamic processing
#
# Dependencies: Azure Data Lake Storage (ADLS) Gen2
# ==============================================================================

# MAGIC %md
# MAGIC ### **Dynamic Capabilities**

# COMMAND ----------

# Accept file name parameter passed from orchestration jobs
dbutils.widgets.text("file_name", "")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

# Auto Loader detects schema automatically and handles schema evolution
# cloudFiles.schemaLocation stores inferred schema for future runs
df = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation", f"abfss://bronze@databrickseteloua.dfs.core.windows.net/checkpoint_{p_file_name}")\
                .load(f"abfss://source@databrickseteloua.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

# Append mode ensures incremental data loads without duplication
# availableNow=True processes all available data immediately (microbatch mode)
# Checkpoint enables recovery from failures without reprocessing data
df.writeStream.format("parquet")\
                .outputMode("append")\
                .option("checkpointLocation", f"abfss://bronze@databrickseteloua.dfs.core.windows.net/checkpoint_{p_file_name}")\
                .option("path", f"abfss://bronze@databrickseteloua.dfs.core.windows.net/{p_file_name}")\
                .trigger(availableNow=True)\
                .start()
