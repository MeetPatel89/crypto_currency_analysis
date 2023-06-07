# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, TimestampType
from pyspark.sql.functions import from_unixtime, date_format, row_number, lit, current_timestamp, weekofyear
from pyspark.sql.window import Window

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
hist_data = spark.read.option("header", True).schema(schema).csv(hist_csv_path)

hist_data.printSchema()


# COMMAND ----------

# create date dimension table
date_dim = hist_data.select("time").dropDuplicates(["time"])

# COMMAND ----------

date_dim = date_dim.withColumnRenamed("time", "unix_epoch")
date_dim = date_dim.withColumn("unix_datetime", from_unixtime("unix_epoch", "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
date_dim = date_dim.withColumns(
    {
        "day_name": date_format("unix_datetime", "EEEE"),
        "month_name": date_format("unix_datetime", "MMMM"),
        "year": date_format("unix_datetime", "yyyy"),
        "quarter": date_format("unix_datetime", "QQQ"),
        "week_of_year": weekofyear("unix_datetime"),
        "id": row_number().over(Window.partitionBy(lit("")).orderBy(lit(""))),
        "ingestion_date": current_timestamp()
    }
)

# COMMAND ----------

date_dim = date_dim.select("id", "unix_epoch", "unix_datetime", "day_name", "month_name", "quarter", "year", "week_of_year", "ingestion_date")

# COMMAND ----------

date_dim.printSchema()

# COMMAND ----------

display(date_dim)

# COMMAND ----------

date_dim_path = f"{processed_cont_path}/date_dim.parquet"
date_dim.write.mode("Overwrite").parquet(date_dim_path)

# COMMAND ----------

a = spark.read.parquet(date_dim_path)
a.sort("unix_epoch", ascending=False).show(20)

# COMMAND ----------

a.printSchema()

# COMMAND ----------

dbutils.notebook.exit("Success")
