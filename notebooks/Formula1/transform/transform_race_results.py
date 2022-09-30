# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform race_results
# MAGIC ##### Create a DataFrame containing race results

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read from the required parquet files from processed layer using DataFrame Reader API

# COMMAND ----------

races_df = read_parquet_file(processed_folder_path, "races")

# COMMAND ----------

circuits_df = read_parquet_file(processed_folder_path, "circuits")

# COMMAND ----------

drivers_df = read_parquet_file(processed_folder_path, "drivers")

# COMMAND ----------

results_df = read_parquet_file(processed_folder_path, "results")

# COMMAND ----------

constructors_df = read_parquet_file(processed_folder_path, "constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Join races_df with circuits_df and select only the required fields

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
                            .select(col("race_id"), col("race_year"), races_df.name.alias("race_name"), col("race_timestamp").alias("race_date") \
                            ,col("location").alias("circuit_location"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Join results DataFrame with drivers and constructors and select only the required columns

# COMMAND ----------

results_driver_constr_df = results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                                     .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) \
                                     .select(col("race_id"), drivers_df.name.alias("driver_name"), drivers_df.nationality.alias("driver_nationality") \
                                     , drivers_df.number.alias("driver_number"), constructors_df.name.alias("team"), col("grid"), col("fastest_lap") \
                                     , col("time").alias("race_time"), col("points"), col("position")) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Join both the above dataframes

# COMMAND ----------

race_results_df = races_circuits_df.join(results_driver_constr_df, results_driver_constr_df.race_id == races_circuits_df.race_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Drop unwanted columns and add new column: created_date

# COMMAND ----------

race_results_final_df = race_results_df.drop("race_id") \
                                       .withColumn("created_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 6 - Write the transformed dataframe to the presentation layer in parquet format

# COMMAND ----------

race_results_final_df.write.parquet(f"{presentation_folder_path}/race_results", mode = "overwrite")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/race_results"))