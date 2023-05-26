# Databricks notebook source
symbol_dim_run = dbutils.notebook.run("create_symbol_dim", 0)

# COMMAND ----------

if symbol_dim_run == "Success":
    date_dim_run = dbutils.notebook.run("create_date_dim", 0)
else:
    print("Create Symbol dim notebook couldn't run successfully!")

# COMMAND ----------

if date_dim_run == "Success":
    dbutils.notebook.run("create_coin_metrics_fact", 0)
else:
    print("Create Date dim notebook couldn't run successfully!")
