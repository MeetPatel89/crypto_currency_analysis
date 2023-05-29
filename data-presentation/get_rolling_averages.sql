-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC   "spark.sql.legacy.allowNonEmptyLocationInCTAS", "true"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC date_dim = spark.read.parquet(f"{processed_cont_path}/date_dim.parquet")
-- MAGIC display(date_dim)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC symbol_dim = spark.read.parquet(f"{processed_cont_path}/symbol_dim.parquet")
-- MAGIC display(symbol_dim)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC coin_metrics_fact = spark.read.parquet(f"{processed_cont_path}/coin_metrics_fact.parquet")
-- MAGIC display(coin_metrics_fact)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC metrics_sym = coin_metrics_fact.join(symbol_dim, coin_metrics_fact["symbol_id"] == symbol_dim["id"], "inner").select("FullName", "Name", "close", "date_id")
-- MAGIC closing_price = metrics_sym.join(date_dim, metrics_sym["date_id"] == date_dim["id"]).select("unix_datetime", "month_name", "year", "FullName", "Name", "close").withColumnRenamed("unix_datetime", "datetime")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(closing_price)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC closing_price.printSchema()

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS metrics

-- COMMAND ----------

drop table IF EXISTS metrics.closing_price

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC closing_price.write.mode("Overwrite").format("parquet").option("path", f"{presentation_cont_path}/closing_price").saveAsTable("metrics.closing_price")

-- COMMAND ----------

DESC EXTENDED metrics.closing_price

-- COMMAND ----------

DROP TABLE IF EXISTS metrics.rolling_avg_five_day;
CREATE EXTERNAL TABLE metrics.rolling_avg_five_day
 USING PARQUET
  LOCATION "/mnt/cryptoanalysisdl/presentation/rolling_avg_five_day"
AS
select
cast(datetime as date) date,
FullName, Name,
avg(close) over(
  partition by Name 
  order by datetime desc 
  rows between current row and 4 following) closing_rolling5
 from metrics.closing_price;

-- COMMAND ----------

DROP TABLE IF EXISTS metrics.monthly_averages;
CREATE EXTERNAL TABLE metrics.monthly_averages
 USING PARQUET
  LOCATION "/mnt/cryptoanalysisdl/presentation/monthly_averages"
as
select 
year, month_name, month(datetime) month_number, name,
avg(close)
 from metrics.closing_price
 group by year, month_name, month(datetime), name
 order by year desc, name;

-- COMMAND ----------

SELECT * FROM metrics.rolling_avg_five_day

-- COMMAND ----------

SELECT * FROM metrics.monthly_averages

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")
