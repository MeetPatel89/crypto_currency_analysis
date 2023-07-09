-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # to allow for creation of external tables
-- MAGIC spark.conf.set(
-- MAGIC   "spark.sql.legacy.allowNonEmptyLocationInCTAS", "true"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC date_dim = spark.read.format("delta").load(f"{processed_cont_path}/date_dim")
-- MAGIC display(date_dim)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC symbol_dim = spark.read.format("delta").load(f"{processed_cont_path}/symbol_dim")
-- MAGIC display(symbol_dim)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC coin_metrics_fact = spark.read.format("delta").load(f"{processed_cont_path}/coin_metrics_fact")
-- MAGIC display(coin_metrics_fact)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC metrics_sym = coin_metrics_fact.join(symbol_dim, coin_metrics_fact["symbol_id"] == symbol_dim["id"], "inner").select("FullName", "Name", "close", "date_id")
-- MAGIC closing_price = metrics_sym.join(date_dim, metrics_sym["date_id"] == date_dim["id"]).select("unix_datetime", "month_name", "year", "week_of_year", "FullName", "Name", "close").withColumnRenamed("unix_datetime", "datetime")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(closing_price)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC closing_price.printSchema()

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS metrics

-- COMMAND ----------

DROP TABLE IF EXISTS metrics.closing_price

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC closing_price.write.mode("Overwrite").format("parquet").saveAsTable("metrics.closing_price")

-- COMMAND ----------

DESC EXTENDED metrics.closing_price

-- COMMAND ----------

SELECT 
*
FROM
metrics.closing_price

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/cryptoanalysisdl/presentation/rolling_averages",recurse=True)

-- COMMAND ----------

-- create external table for rolling averages over 5 days, 7 days, 10 days, 15 days and 30 days for dashboarding
DROP TABLE IF EXISTS metrics.rolling_averages;
CREATE EXTERNAL TABLE metrics.rolling_averages
USING DELTA
LOCATION "/mnt/cryptoanalysisdl/presentation/rolling_averages"
AS
SELECT 
cast(datetime as DATE) date,
FullName, Name,
close closing_price,
-- 5 day rolling average
avg(close) OVER (
  PARTITION BY Name 
  ORDER BY datetime DESC
  ROWS BETWEEN CURRENT ROW and 4 FOLLOWING
) rolling_5_day,
-- 7 day rolling average
avg(close) OVER (
  PARTITION BY Name 
  ORDER BY datetime DESC
  ROWS BETWEEN CURRENT ROW and 6 FOLLOWING
) rolling_7_day,
-- 10 day rolling average
avg(close) OVER (
  PARTITION BY Name 
  ORDER BY datetime DESC
  ROWS BETWEEN CURRENT ROW and 9 FOLLOWING
) rolling_10_day,
-- 15 day rolling average
avg(close) OVER (
  PARTITION BY Name 
  ORDER BY datetime DESC
  ROWS BETWEEN CURRENT ROW and 14 FOLLOWING
) rolling_15_day,
-- 30 day rolling average
avg(close) OVER (
  PARTITION BY Name 
  ORDER BY datetime DESC
  ROWS BETWEEN CURRENT ROW and 29 FOLLOWING
) rolling_30_d
FROM
metrics.closing_price

-- COMMAND ----------

SELECT * FROM metrics.rolling_averages

-- COMMAND ----------

-- drop closing_price - this will delete both metadata and actual data as it's managed table
DROP TABLE IF EXISTS metrics.closing_price

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")

-- COMMAND ----------


