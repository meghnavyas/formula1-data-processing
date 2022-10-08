# Databricks notebook source
# MAGIC %md
# MAGIC ### Driver Standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the race_results file from presentation layer

# COMMAND ----------

# MAGIC %md
# MAGIC Find the race year(s) for we have received the data in current load

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------



# COMMAND ----------

curr_race_years = column_to_list(race_results_df, "race_year")
curr_race_years

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
                       .filter(col("race_year").isin(curr_race_years))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Present the drivers with their total points scored and wins, year-wise 

# COMMAND ----------

from pyspark.sql.functions import count, col, sum, when, desc

# COMMAND ----------

driver_standings_df = race_results_df.groupBy(col("driver_name"), col("race_year"), col("driver_nationality")) \
                                     .agg(sum("points").alias("points"), count(when((col("position") == 1), True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year = 2019").orderBy(desc("wins")))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Add new column: rank

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import rank, desc

# COMMAND ----------

driver_window = Window.partitionBy("race_year").orderBy(desc("points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_window))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to delta table in presentation layer

# COMMAND ----------

#overwrite_partition(final_df, "f1_presentation", "driver_standings", "race_year")
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, "f1_presentation", "driver_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from f1_presentation.driver_standings;