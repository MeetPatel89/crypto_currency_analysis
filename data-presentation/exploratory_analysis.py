# Databricks notebook source
from pyspark.sql.functions import col, max, min, corr

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

date_dim = spark.read.parquet(f"{processed_cont_path}/date_dim.parquet")

# COMMAND ----------

symbol_dim = spark.read.parquet(f"{processed_cont_path}/symbol_dim.parquet")

# COMMAND ----------

coin_metrics_fact = spark.read.parquet(f"{processed_cont_path}/coin_metrics_fact.parquet")

# COMMAND ----------

#compute summary statistics for numeric columns
summary_stats = coin_metrics_fact.select("open", "close", "low", "high", "volumefrom", "volumeto").describe()

# COMMAND ----------

# get all relevant columns from data model for answering analytical questions

coin_metrics_date = coin_metrics_fact.join(date_dim, coin_metrics_fact["date_id"] == date_dim["id"], "inner").select("symbol_id", "unix_datetime", "year", "month_name", "open", "close", "low", "high", "volumefrom", "volumeto").withColumnRenamed("unix_datetime", "datetime")
df = coin_metrics_date.join(symbol_dim, coin_metrics_date["symbol_id"] == symbol_dim["id"], "inner").select("name", "datetime", "year", "month_name", "open", "close", "low", "high", "volumefrom", "volumeto")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volumeto (value of coin in USD) of coin traded for a day

# COMMAND ----------

df_hv = df.withColumn("HV_Ratio", col("high") / col("volumeto")).fillna(0) 

# get HV Ratio for records with recent dates
display(df_hv.sort("datetime", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC #### What day had the Peak High in Price for each cryptocurrency in dataset?

# COMMAND ----------

# get coin names groupby object for later aggregations
coins_groupby_obj = df.groupby("name")

# COMMAND ----------

max_high = coins_groupby_obj.max("high").withColumnRenamed("name", "coin_name")
max_high = df.join(max_high, (df["high"] == max_high["max(high)"]) & (max_high["coin_name"] == df["name"]), "inner").sort("high", ascending=False).select("name", "datetime", "high")
max_high.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the mean closing price for each cryptocurrency?

# COMMAND ----------

avg_close = coins_groupby_obj.mean("close")
avg_close.sort("avg(close)", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What are max and min for volumes (both crypto units/volumefrom and USD value/volumeto) for all cryptocurrencies in dataset?

# COMMAND ----------

coins_groupby_obj.agg(
    max("volumefrom").alias("max(crypto_units)"),
    min("volumefrom").alias("min(crypto_units)"),
    max("volumeto").alias("max(crypto value - USD)"),
    min("volumeto").alias("min(crypto value - USD)")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is Pearson's Correlation Coefficient for high and volumeto (USD Value) for each cryptocurrency?

# COMMAND ----------

coins_groupby_obj.agg(
    corr("high", "volumeto")
).sort("corr(high, volumeto)", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### What are max metrics per year for each cryptocurrency?

# COMMAND ----------

yearly_max = df.groupby(["name", "year"]).agg(
                max("open"),
                max("close"),
                max("high"),
                max("low")
            ).sort(["year", "name"], ascending=False)
display(yearly_max)

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is the average monthly closing price for each cryptocurrency?

# COMMAND ----------

avg_monthly_close = df.groupby(["name", "year", "month_name"]).avg("close")
display(avg_monthly_close)
