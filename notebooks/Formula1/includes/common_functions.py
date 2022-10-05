# Databricks notebook source
# Including all the commonly used functions for below ETL activities:
# 1. Add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    return input_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# Method to read given parquet file from given folder
def read_parquet_file(mount_path, folder):
    return spark.read.parquet(f"{mount_path}/{folder}")

# COMMAND ----------

# Method to put partition column from the final dataframe at the end
def rearrange_columns(input_df, partition):
    column_list = []
    for col in input_df.schema.names:
        if col != partition:
            column_list.append(col)
    column_list.append(partition)
    return input_df.select(column_list)

# COMMAND ----------

# Method to write the partition dataset to destination folder in overwrite mode and parquet format + table
def overwrite_partition(input_df, db_nm, tbl_nm, partition_col):
    
    # Set partition overwrite mode to "dynamic": so its able to figure out the current partition data being loaded and delete it in case of reruns
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    # If table already exists, go for delta run, else initial load
    if (spark._jsparkSession.catalog().tableExists(f"{db_nm}.{tbl_nm}")):
        input_df.write.mode("overwrite").insertInto(f"{db_nm}.{tbl_nm}")
    else:
        input_df.write.mode("overwrite").partitionBy(partition_col).format("parquet").saveAsTable(f"{db_nm}.{tbl_nm}")
    return "SUCCESS"