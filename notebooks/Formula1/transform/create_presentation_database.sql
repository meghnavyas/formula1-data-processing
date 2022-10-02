-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create presentaion database
-- MAGIC Store managed tables in the presentation layer built on top of transformed parquet files

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/2022formula1dl/presentation";