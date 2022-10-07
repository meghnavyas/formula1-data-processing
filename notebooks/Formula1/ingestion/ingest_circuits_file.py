# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

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

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", header = True, schema = circuits_schema)

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
# MAGIC ##### Step 4 - Add a new columns; file_date, data_source, ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_col_added_df = circuits_renamed_df.withColumn("data_source", lit(v_data_source)) \
                                           .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_col_added_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 5 - Write the DataFrame to a Delta table in ADLS

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from f1_processed.circuits;