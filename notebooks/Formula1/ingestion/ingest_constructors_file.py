# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Define schema for the constructors DataFrame (using DDL method here) and read into the DataFrame from constructors.json file

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.json(f"{raw_folder_path}/constructors.json", schema = constructors_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop the unwanted column: url

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns and add a new column: ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                                 .withColumnRenamed("constructorRef", "constructor_ref") \
                                                 .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the dataframe into a parquet file in ADLS

# COMMAND ----------

constructors_final_df.write.parquet(f"{processed_folder_path}constructors", mode = "overwrite")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}constructors"))