-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC symbol_dim_path = f"{processed_cont_path}/symbol_dim.parquet"
-- MAGIC date_dim_path = f"{processed_cont_path}/date_dim.parquet"
-- MAGIC coin_metrics_fact_path = f"{processed_cont_path}/coin_metrics_fact.parquet"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC date_dim = spark.read.parquet(date_dim_path)
-- MAGIC display(date_dim)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC symbol_dim = spark.read.parquet(symbol_dim_path)
-- MAGIC display(symbol_dim)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC coin_metrics_fact = spark.read.parquet(coin_metrics_fact_path)
-- MAGIC display(coin_metrics_fact)

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS base_tables

-- COMMAND ----------

DROP TABLE IF EXISTS base_tables.date_dim

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC date_dim.write.mode("Overwrite").format("parquet").saveAsTable("base_tables.date_dim")

-- COMMAND ----------

DROP TABLE IF EXISTS base_tables.symbol_dim

-- COMMAND ----------

-- MAGIC %python
-- MAGIC symbol_dim.write.mode("Overwrite").format("parquet").saveAsTable("base_tables.symbol_dim")

-- COMMAND ----------

DROP TABLE IF EXISTS base_tables.coin_metrics_fact

-- COMMAND ----------

-- MAGIC %python
-- MAGIC coin_metrics_fact.write.mode("Overwrite").format("parquet").saveAsTable("base_tables.coin_metrics_fact")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")
