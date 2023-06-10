# Databricks notebook source
base_tables_run = dbutils.notebook.run("get_base_tables", 0)

# COMMAND ----------

if base_tables_run == "Success":
    rolling_avgs_run = dbutils.notebook.run("get_rolling_averages", 0)
else:
    print("Get base tables notebook couldn't run successfully!")

# COMMAND ----------

if rolling_avgs_run == "Success":
    dbutils.notebook.run("exploratory_analysis", 0)
else:
    print("Get rolling averages notebook couldn't run successfully!")
