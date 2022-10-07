# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying split multiline JSON files

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
# MAGIC ##### Step 1 - Define schema and read the split multiline JSON files using spark DataFrame reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType([StructField("qualifyId", IntegerType(), False),
                              StructField("raceId", IntegerType(), False),
                              StructField("driverId", IntegerType(), False),
                              StructField("constructorId", IntegerType(), False),
                              StructField("number", IntegerType(), False),
                              StructField("position", IntegerType(), False),
                              StructField("q1", StringType(), True),
                              StructField("q2", StringType(), True),
                              StructField("q3", StringType(), True),
                            ])

# COMMAND ----------

qualifying_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/qualifying/qualifying_split*.json", multiLine = True, schema = qualifying_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename the columns and add new columns: data source, file_date, ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("constructorId", "constructor_id") \
                               .withColumnRenamed("qualifyId", "qualify_id") \
                               .withColumn("data_source", lit(v_data_source)) \
                               .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the DataFrame into processed container in a parquet format

# COMMAND ----------

#overwrite_partition(qualifying_final_df, "f1_processed", "qualifying", "race_id")

# COMMAND ----------

# Initial/ Delta load to Delta Table
merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, "f1_processed", "qualifying", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1)
# MAGIC   FROM f1_processed.qualifying
# MAGIC   GROUP BY 1
# MAGIC   ORDER BY 1 DESC;