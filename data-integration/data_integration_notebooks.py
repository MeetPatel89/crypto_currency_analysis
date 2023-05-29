# Databricks notebook source
get_top10_coins_run = dbutils.notebook.run("get_api_data_top10_coins", 0)

# COMMAND ----------

if get_top10_coins_run == "Success":
    dbutils.notebook.run("get_api_data_history", 0)
else:
    print("Get api data top 10 coins notebook couldn't run successfully!")
