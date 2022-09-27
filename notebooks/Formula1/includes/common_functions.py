# Databricks notebook source
# MAGIC %md
# MAGIC ##### Including all the commonly used functions for below ETL activities:
# MAGIC 1. Add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    return input_df.withColumn("ingestion_date", current_timestamp())