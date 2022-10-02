-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create a database for the processed layer
-- MAGIC Provide ADLS location to create Managed tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/2022formula1dl/processed";