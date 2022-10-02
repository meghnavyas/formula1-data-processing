# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying split multiline JSON files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

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

qualifying_df = spark.read.json(f"{raw_folder_path}/qualifying/qualifying_split*.json", multiLine = True, schema = qualifying_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename the columns and add a new column: ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("raceId", "race_id") \
                               .withColumnRenamed("constructorId", "constructor_id") \
                               .withColumnRenamed("qualifyId", "qualify_id") \
                               .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the DataFrame into processed container in a parquet format

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")