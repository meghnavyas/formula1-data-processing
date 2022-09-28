# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times split csv files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

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

laptimes_df = spark.read.csv(f"{raw_folder_path}/lap_times/lap_times_split*.csv", schema = laptimes_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename the columns and add a new column: ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

laptimes_renamed_df = laptimes_df.withColumnRenamed("driverId", "driver_id") \
                                 .withColumnRenamed("raceId", "race_id") \
                                 .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

laptimes_final_df = add_ingestion_date(laptimes_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the DataFrame into processed container in a parquet format

# COMMAND ----------

laptimes_final_df.write.parquet(f"{processed_folder_path}/lap_times", mode = "overwrite")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))