# Databricks notebook source
from pyspark.sql.functions import row_number, lit, current_timestamp
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# read from top 10 coins json in raw container

top10_coins_json_path = f"{raw_cont_path}/top10_coins.json"
df_top10 = spark.read.json(top10_coins_json_path)

df_top10 = df_top10.withColumns({
                "id": row_number().over(Window.partitionBy(lit("")).orderBy(lit(""))),
                "ingestion_date": current_timestamp()
}

)

symbol_dim = df_top10.select("id", "Name", "FullName", "Algorithm", "ProofType", "ingestion_date")

# COMMAND ----------

display(symbol_dim)

# COMMAND ----------

symbol_dim.printSchema()

# COMMAND ----------

symbol_dim_path = f"{processed_cont_path}/symbol_dim.parquet"
symbol_dim.write.mode("Overwrite").parquet(symbol_dim_path)

# COMMAND ----------

a = spark.read.parquet(symbol_dim_path)
a.printSchema()

# COMMAND ----------

dbutils.notebook.exit("Success")
