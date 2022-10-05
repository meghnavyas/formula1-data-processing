# Databricks notebook source
# MAGIC %md
# MAGIC ### Constructor Standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Find the race years for which the data is to be processed

# COMMAND ----------

from pyspark.sql.functions import col

race_years_list = read_parquet_file("/mnt/2022formula1dl/presentation","race_results") \
                  .select(col("race_year")) \
                  .distinct() \
                  .collect()

# COMMAND ----------

curr_race_years = column_to_list(race_years_list)

# COMMAND ----------

# Read the race_results parquet files from presentation folder
race_results_df = read_parquet_file(presentation_folder_path, "race_results") \
                       .filter(col("race_year").isin(curr_race_years))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating constructors standings DataFrame having following attributes:
# MAGIC 1. race_year
# MAGIC 2. team
# MAGIC 3. wins
# MAGIC 4. points
# MAGIC 5. rank

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

# COMMAND ----------

# Adding wins and points columns
constructor_standings_df = race_results_df.groupBy("race_year", "team") \
                                       .agg(sum("points").alias("points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import rank, desc

# COMMAND ----------

# Add rank column based on wins and points
rank_window = Window.partitionBy("race_year").orderBy(desc("wins"), desc("points"))
final_df = constructor_standings_df.withColumn("rank", rank().over(rank_window))

# COMMAND ----------

# Write it into presentation layer in parquet format and create table on top of it
overwrite_partition(final_df, "f1_presentation", "constructor_standings", "race_year")