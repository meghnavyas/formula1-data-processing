-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

-- Create table and store in presentation layer

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT rc.race_year,
       c.name as team_name,
       d.name as driver_name,
       r.position,
       r.points,
       11 - r.position as calculated_points
    FROM f1_processed.results r
    JOIN f1_processed.drivers d
      ON d.driver_id = r.driver_id
    JOIN f1_processed.races rc
      ON rc.race_id = r.driver_id
    JOIN f1_processed.constructors c
      ON c.constructor_id = r.constructor_id
  WHERE r.position <= 10;