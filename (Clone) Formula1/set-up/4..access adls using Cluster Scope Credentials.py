# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using Cluster Scope Credentials
# MAGIC 1. Set the spark config fs.azure.account.key in the cluster 
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from ciruit.csv file
# MAGIC

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1datalake24.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1datalake24.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake24.dfs.core.windows.net/002 circuits.csv"),header = True)

# COMMAND ----------


