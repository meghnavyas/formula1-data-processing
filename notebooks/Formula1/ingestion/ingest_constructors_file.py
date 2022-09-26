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

# MAGIC %md
# MAGIC ##### Step 2 - Drop the unwanted column: url

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns and add a new column: ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                               .withColumnRenamed("constructorRef", "constructor_ref") \
                                               .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the dataframe into a parquet file in ADLS

# COMMAND ----------

constructors_final_df.write.parquet("/mnt/2022formula1dl/processed/constructors", mode = "overwrite")