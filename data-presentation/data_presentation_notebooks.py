# Databricks notebook source
rolling_avgs_run = dbutils.notebook.run("get_rolling_averages", 0)

# COMMAND ----------

if rolling_avgs_run == "Success":
    dbutils.notebook.run("exploratory_analysis", 0)
else:
    print("Get rolling averages notebook couldn't run successfully!")
