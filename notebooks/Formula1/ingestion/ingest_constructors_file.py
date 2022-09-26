# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Define schema for the constructors DataFrame (using DDL method here) and read into the DataFrame from constructors.json file

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.json("/mnt/2022formula1dl/raw/constructors.json", schema = constructors_schema)

# COMMAND ----------

