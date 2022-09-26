# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

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

pitstops_df = spark.read.json("/mnt/2022formula1dl/raw/pit_stops.json", multiLine = True, schema = pitstops_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename the columns and add a new column: ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pitstops_final_df = pitstops_df.withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("raceId", "race_id") \
                               .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the DataFrame into processed container in a parquet format

# COMMAND ----------

pitstops_final_df.write.parquet("/mnt/2022formula1dl/processed/pit_stops", mode = "overwrite")