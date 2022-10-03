-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results;

-- COMMAND ----------

Select team_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as avg_points
   FROM f1_presentation.calculated_race_results
   GROUP BY 1
   HAVING COUNT(1) >= 100
   ORDER BY avg_points DESC;