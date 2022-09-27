# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Define schema for results DataFrame and read results.json file into it

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType([StructField("constructorId", IntegerType(), False),
                             StructField("driverId", IntegerType(),False),
                             StructField("fastestLap", IntegerType(),True),
                             StructField("fastestLapSpeed", StringType(),True),
                             StructField("fastestLapTime", StringType(),True),
                             StructField("grid", IntegerType(),True),
                             StructField("laps", IntegerType(),True),
                             StructField("milliseconds", StringType(),True),
                             StructField("number", IntegerType(),True),
                             StructField("points", FloatType(),True),
                             StructField("positionOrder", IntegerType(),True),
                             StructField("positionText", StringType(),True),
                             StructField("raceId", IntegerType(),True),
                             StructField("rank", IntegerType(),True),
                             StructField("resultId", IntegerType(),True),
                             StructField("statusId", IntegerType(),True),
                             StructField("time", StringType(),True)
                           ])

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/results.json", schema = results_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("constructorId", "constructor_id") \
                               .withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("fastestLap", "fastest_lap") \
                               .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                               .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                               .withColumnRenamed("positionOrder", "position_order") \
                               .withColumnRenamed("positionText", "position_text") \
                               .withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("resultId", "result_id")

# COMMAND ----------

results_col_added_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3 - Remove unwanted column: statusId

# COMMAND ----------

results_final_df = results_col_added_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the dataframe to processed container in paraquet format

# COMMAND ----------

results_final_df.write.parquet(f"{processed_folder_path}/results", mode = "overwrite")