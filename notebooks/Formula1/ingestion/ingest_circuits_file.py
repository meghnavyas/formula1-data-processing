# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Define a schema for the DataFrame circuits_df

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType([StructField("circuitId", IntegerType(), False),
                             StructField("circuitRef", StringType(), True),
                             StructField("name", StringType(), True),
                             StructField("location", StringType(), True),
                             StructField("country", StringType(), True),
                             StructField("lat", DoubleType(), True),
                             StructField("lng", DoubleType(), True),
                             StructField("alt", IntegerType(), True),
                             StructField("url", StringType(), True)
                            ])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Read the circuits.csv file using Spark DataFrame reader with the above defined schema

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/circuits.csv", header = True, schema = circuits_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only required columns and rename the selected columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_renamed_df = circuits_df.select(col("circuitId").alias("circuit_id"), col("circuitRef").alias("circuit_ref"), col("name"),
                                         col("location"), col("country"), col("lat").alias("latitude"), col("lng").alias("longitude"),
                                         col("alt").alias("altitude"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add a new column; ingestion_date to the DataFrame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 5 - Write the DataFrame to a parquet file in ADLS

# COMMAND ----------

circuits_final_df.write.parquet(f"{processed_folder_path}/circuits", mode = "overwrite")