# Databricks notebook source
# ==============================================================================
# Bronze Layer - Job Parameters
# ==============================================================================
# Purpose: Defines the list of datasets to be processed in the Bronze layer.
#          These parameters are passed to orchestration jobs for dynamic 
#          processing of multiple data sources.
#
# Usage: Output datasets are stored as task values for downstream job tasks
# ==============================================================================

# Define datasets to be processed in the Bronze layer
# Each dataset entry corresponds to a separate data source file
datasets = [
    {
        "file_name":"customers"
    },
    {
        "file_name":"orders"
    },
    {
        "file_name":"products"
    }
]

# COMMAND ----------

# Store datasets as job task values for downstream orchestration tasks
dbutils.jobs.taskValues.set("output_datasets", datasets)
