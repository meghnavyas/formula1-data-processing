# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

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
# MAGIC ##### Step 1 - Define schema for drivers DataFrame and read the drivers.json file into it

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# Defining schema for the nested name in drivers.json
name_schema = StructType([StructField("forename", StringType(), True),
                          StructField("surname", StringType(), True)
                        ])

# COMMAND ----------

# Defining schema for drivers dataframe
drivers_schema = StructType([StructField("code", StringType(), True),
                             StructField("dob", DateType(), True),
                             StructField("driverId", IntegerType(), False),
                             StructField("driverRef", StringType(), True),
                             StructField("name", name_schema, True),
                             StructField("nationality", StringType(), True),
                             StructField("number", IntegerType(), True),
                             StructField("url", StringType(), True)
                            ])

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/drivers.json", schema = drivers_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add new columns and rename the selected columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverRef renamed to driver_ref
# MAGIC 3. ETL column ingestion_date added holding the current timestamp value
# MAGIC 4. New column name created by concatenating forname and surname
# MAGIC 5. New columns: data_source, file_date (parameters)

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                               .withColumnRenamed("driverRef", "driver_ref")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, concat, lit

# COMMAND ----------

# Since name column already exists, when we create name column using concat the existing json object{forename, surnme} will be dropped
drivers_col_added_df = drivers_renamed_df.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                         .withColumn("data_source", lit(v_data_source)) \
                                         .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_ingested_df = add_ingestion_date(drivers_col_added_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Remove unwanted column: url

# COMMAND ----------

drivers_final_df = drivers_ingested_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the DataFrame to processed contained in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")