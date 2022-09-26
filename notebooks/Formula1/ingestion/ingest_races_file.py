# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv file

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 1 - Define schema for races DataFrame

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

races_schema = StructType([StructField("raceId", IntegerType(), False),
                           StructField("year", IntegerType(), True),
                           StructField("round", IntegerType(), True),
                           StructField("circuitId", IntegerType(), True),
                           StructField("name", StringType(), True),
                           StructField("date", StringType(), True),
                           StructField("time", StringType(), True),
                           StructField("url", StringType(), True)
                          ])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Read races.csv file into DataFrame with the above described schema

# COMMAND ----------

races_df = spark.read.csv("/mnt/2022formula1dl/raw/races.csv", header = True, schema = races_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the required columns and rename them

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_renamed_df = races_df.select(col("raceId").alias("race_id"),
                                   col("year").alias("race_year"),
                                   col("round"),
                                   col("circuitId").alias("circuit_id"),
                                   col("name"),
                                   col("date"),
                                   col("time")
                                  )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Merge date and time columns to create a new column: race_timestamp

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit, current_timestamp

# COMMAND ----------

races_renamed_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Add a new ETL column: ingestion date

# COMMAND ----------

races_col_added_df = races_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 6 - Remove date and time columns

# COMMAND ----------

races_final_df = races_col_added_df.select(col("race_id"),
                                                col("race_year"),
                                                col("round"),
                                                col("circuit_id"),
                                                col("name"),
                                                col("race_timestamp"),
                                                col("ingestion_date")
                                               )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 7 - Write the races DataFrame to a parquet file in mounted ADLS

# COMMAND ----------

races_final_df.write.parquet("/mnt/2022formula1dl/processed/races", mode = "overwrite")