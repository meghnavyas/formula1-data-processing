# Databricks notebook source
# Including all the commonly used functions for below ETL activities:
# 1. Add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    return input_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# Function to read given parquet file from given folder
def read_parquet_file(mount_path, folder):
    return spark.read.parquet(f"{mount_path}/{folder}")