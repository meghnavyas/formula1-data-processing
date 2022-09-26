# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times split csv files

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Define schema and read the split csv files using spark DataFrame reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

laptimes_schema = StructType([StructField("raceId", IntegerType(), False),
                              StructField("driverId", IntegerType(), False),
                              StructField("lap", IntegerType(), False),
                              StructField("position", IntegerType(), False),
                              StructField("time", StringType(), False),
                              StructField("milliseconds", IntegerType(), False)
                            ])

# COMMAND ----------

laptimes_df = spark.read.csv("/mnt/2022formula1dl/raw/lap_times/lap_times_split*.csv", schema = laptimes_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename the columns and add a new column: ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

laptimes_final_df = laptimes_df.withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("raceId", "race_id") \
                               .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the DataFrame into processed container in a parquet format

# COMMAND ----------

laptimes_final_df.write.parquet("/mnt/2022formula1dl/processed/lap_times", mode = "overwrite")