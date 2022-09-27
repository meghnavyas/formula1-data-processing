# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Define schema and read the multiline JSON file using spark DataFrame reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pitstops_schema = StructType([StructField("driverId", IntegerType(), False),
                              StructField("duration", StringType(), True),
                              StructField("lap", IntegerType(), False),
                              StructField("milliseconds", IntegerType(), False),
                              StructField("raceId", IntegerType(), False),
                              StructField("stop", IntegerType(), False),
                              StructField("time", StringType(), False)
                            ])

# COMMAND ----------

pitstops_df = spark.read.json(f"{raw_folder_path}/pit_stops.json", multiLine = True, schema = pitstops_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename the columns and add a new column: ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pitstops_renamed_df = pitstops_df.withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("raceId", "race_id")

# COMMAND ----------

pitstops_final_df = add_ingestion_date(pitstops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the DataFrame into processed container in a parquet format

# COMMAND ----------

pitstops_final_df.write.parquet(f"{processed_folder_path}/pit_stops", mode = "overwrite")