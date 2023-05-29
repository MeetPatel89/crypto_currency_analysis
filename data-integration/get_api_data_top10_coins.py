# Databricks notebook source
# MAGIC %md
# MAGIC #### Get api data for top 10 coins
# MAGIC 1. Make API call to get top 10 coins 
# MAGIC 2. Store top 10 coins in json in raw container
# MAGIC 3. Store json data in csv format

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/api_functions"

# COMMAND ----------

from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# COMMAND ----------

# authorization header for making calls to crypto compare API
# read api key from azure key vault

api_key = dbutils.secrets.get(scope="crypto-analysis-scope",key="apiKey")

headers = {
  "authorization": f"Apikey {api_key}"
}

# COMMAND ----------

# relevant urls for making calls to crypto compare API
top10_url = "https://min-api.cryptocompare.com/data/top/mktcapfull"

# get top 10 coins by market cap, capture json response
parameters = {
  "tsym":"USD",
  "limit": 10
}

res_json_top10 = get_data(top10_url, parameters, headers)
data_top10 = res_json_top10["Data"]

# COMMAND ----------

# capture top 10 coins in a list of dictionaries 
top10_coins_json_path = f"{raw_cont_path}/top10_coins.json"
top10_coins = [{"Name": coin["CoinInfo"]["Name"], "FullName": coin["CoinInfo"]["FullName"], "Algorithm": coin["CoinInfo"]["Algorithm"], "ProofType": coin["CoinInfo"]["ProofType"]} for coin in data_top10]

rdd_json = spark.sparkContext.parallelize(top10_coins)
df_top10_coins = rdd_json.toDF()

# write top10 coins dataframe to json file in raw container for later usage in getting daily history data for respective coins
df_top10_coins.write.mode("Overwrite").json(top10_coins_json_path)


# COMMAND ----------

display(df_top10_coins)

# COMMAND ----------

# to indicate notebook job ran successfully
dbutils.notebook.exit("Success")
