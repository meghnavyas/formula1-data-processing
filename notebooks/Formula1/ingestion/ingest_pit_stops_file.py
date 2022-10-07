# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

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

pitstops_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json", multiLine = True, schema = pitstops_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename the columns and add new columns: data_source, file_date, ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pitstops_renamed_df = pitstops_df.withColumnRenamed("driverId", "driver_id") \
                                 .withColumnRenamed("raceId", "race_id") \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

pitstops_final_df = add_ingestion_date(pitstops_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the DataFrame into processed container in a parquet format

# COMMAND ----------

#overwrite_partition(pitstops_final_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

# Initial/ Delta load to Delta Table
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(pitstops_final_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")