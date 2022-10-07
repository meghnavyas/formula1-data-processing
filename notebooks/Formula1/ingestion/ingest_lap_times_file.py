# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times split csv files

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

laptimes_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv", schema = laptimes_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename the columns and add new columns: data_source, file_date, ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

laptimes_renamed_df = laptimes_df.withColumnRenamed("driverId", "driver_id") \
                                 .withColumnRenamed("raceId", "race_id") \
                                 .withColumn("data_source", lit(v_data_source)) \
                                 .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

laptimes_final_df = add_ingestion_date(laptimes_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the DataFrame into processed container in a parquet format

# COMMAND ----------

#overwrite_partition(laptimes_final_df, "f1_processed", "lap_times", "race_id")
merge_condition = "tgt.race_id = src.race_id"
merge_delta_data(laptimes_final_df, "f1_processed", "lap_times", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC   FROM f1_processed.lap_times
# MAGIC   GROUP BY 1
# MAGIC   ORDER BY race_id DESC;