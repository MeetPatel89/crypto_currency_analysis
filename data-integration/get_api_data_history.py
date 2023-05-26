# Databricks notebook source
# MAGIC %md
# MAGIC #### Get api data for daily history for top 10 coins
# MAGIC 1. Make API call to get daily history data for top 10 coins
# MAGIC 2. Store it in json in raw container
# MAGIC 3. Convert the json history data to csv and store it in raw container

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/api_functions"

# COMMAND ----------

from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

# COMMAND ----------

# authorization header for making calls to crypto compare API
# read api key from azure key vault

api_key = dbutils.secrets.get(scope="crypto-analysis-scope",key="apiKey")

headers = {
  "authorization": f"Apikey {api_key}"
}

# COMMAND ----------

# read from top 10 coins json in raw container

top10_coins_json_path = f"{raw_cont_path}/top10_coins.json"
df_top10 = spark.read.json(top10_coins_json_path)

# collect top 10 coins names in python list
coins = df_top10.rdd.map(lambda x: x.Name).collect()
coins

# COMMAND ----------

# relevant url for getting history daily data for coins
hist_url = "https://min-api.cryptocompare.com/data/histoday"

# read historical data for top 10 coins from cryptocompare API
all_coins_data = get_coin_data(coins, hist_url, headers)


# COMMAND ----------

rdd_all_coins_data = spark.sparkContext.parallelize(all_coins_data)
df_hist_coins = spark.read.json(rdd_all_coins_data)
df_hist_coins = df_hist_coins.select("close", "conversionSymbol", "conversionType", "high", "low", "open", "symbol", "time", "volumefrom", "volumeto")

# COMMAND ----------

# write history data for top 10 coins in csv file in raw container
hist_csv_path = f"{raw_cont_path}/hist_coins.csv"
df_hist_coins.write.mode("Overwrite").option("header", True).csv(hist_csv_path)

# COMMAND ----------

display(df_hist_coins.filter(df_hist_coins["symbol"] == "BTC").sort("time", ascending=False))

# COMMAND ----------

dbutils.notebook.exit("Success")
