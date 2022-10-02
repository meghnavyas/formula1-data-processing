# Databricks notebook source
# MAGIC %md
# MAGIC ### Driver Standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the race_results file from presentation layer

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Present the drivers with their total points scored and wins, year-wise 

# COMMAND ----------

from pyspark.sql.functions import count, col, sum, when, desc

# COMMAND ----------

driver_standings_df = race_results_df.groupBy(col("driver_name"), col("race_year"), col("driver_nationality"), "team") \
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
# MAGIC ##### Step 4 - Write to parquet file in presentation layer

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")