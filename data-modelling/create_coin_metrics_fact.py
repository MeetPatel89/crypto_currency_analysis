# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# read history data from json file in raw container in azure datalake

schema = StructType([
    StructField("close", DoubleType(), True),
    StructField("conversionSymbol", StringType(), True),
    StructField("conversionType", StringType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("open", DoubleType(), True),
    StructField("symbol", StringType(), True),
    StructField("time", LongType(), True),
    StructField("volumefrom", DoubleType(), True),
    StructField("volumeto", DoubleType(), True)
])

hist_csv_path = f"{raw_cont_path}/hist_coins.csv"
df = spark.read.option("header", True).schema(schema).csv(hist_csv_path)

df.printSchema()

# COMMAND ----------

coin_metrics_fact = df.withColumn("high", df["high"].cast("double")).withColumn("low", df["low"].cast("double")).withColumn("open", df["open"].cast("double")).withColumn("close", df["close"].cast("double")).withColumn("volumefrom", df["volumefrom"].cast("double")).withColumn("volumeto", df["volumeto"].cast("double"))

coin_metrics_fact = df.withColumns(
                        {
                            "high": df["high"].cast("double"),
                            "low": df["low"].cast("double"),
                            "open": df["open"].cast("double"),
                            "close": df["close"].cast("double"),
                            "volumefrom": df["volumefrom"].cast("double"),
                            "volumeto": df["volumeto"].cast("double"),
                            "ingestion_date": current_timestamp()
                        }
)
coin_metrics_fact = coin_metrics_fact.select("symbol", "time", "close", "high", "low", "open", "volumefrom", "volumeto", "ingestion_date")

# COMMAND ----------

symbol_dim_path = f"{processed_cont_path}/symbol_dim.parquet"
symbol_dim = spark.read.parquet(symbol_dim_path)

# COMMAND ----------

coin_metrics_fact = coin_metrics_fact.join(symbol_dim.select("id", "Name"), coin_metrics_fact["symbol"] == symbol_dim["Name"], "inner")
coin_metrics_fact = coin_metrics_fact.withColumnRenamed("id", "symbol_id")
coin_metrics_fact = coin_metrics_fact.select("symbol_id", "time", "open", "close", "low", "high", "volumefrom", "volumeto", "ingestion_date")

# COMMAND ----------

coin_metrics_fact.count()

# COMMAND ----------

date_dim_path = f"{processed_cont_path}/date_dim.parquet"
date_dim = spark.read.parquet(date_dim_path)

# COMMAND ----------

coin_metrics_fact = coin_metrics_fact.join(date_dim.select("id", "unix_epoch"), coin_metrics_fact["time"] == date_dim["unix_epoch"], how="inner")
coin_metrics_fact = coin_metrics_fact.withColumnRenamed("id", "date_id")
coin_metrics_fact = coin_metrics_fact.select("date_id", "symbol_id", "open", "close", "low", "high", "volumefrom", "volumeto", "ingestion_date")

# COMMAND ----------

coin_metrics_fact_path = f"{processed_cont_path}/coin_metrics_fact.parquet"
coin_metrics_fact.write.mode("Overwrite").parquet(coin_metrics_fact_path)

# COMMAND ----------

a = spark.read.parquet(coin_metrics_fact_path)
a.printSchema()

# COMMAND ----------

a.show(25, truncate=False)

# COMMAND ----------

dbutils.notebook.exit("Success")
