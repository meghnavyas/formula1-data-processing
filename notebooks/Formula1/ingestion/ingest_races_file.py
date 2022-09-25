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

display(races_df)

# COMMAND ----------

races_df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the required columns and rename them

# COMMAND ----------

from 