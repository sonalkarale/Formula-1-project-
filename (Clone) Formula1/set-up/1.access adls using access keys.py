# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using Keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. LIst files from demo container
# MAGIC 3. Read data from ciruit.csv file
# MAGIC

# COMMAND ----------

formula1dl_account_key=dbutils.secrets.get(scope='formula1-secret-scope',key='formula1-account-key-vault')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1datalake24.dfs.core.windows.net",
    formula1dl_account_key
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1datalake24.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1datalake24.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake24.dfs.core.windows.net/002 circuits.csv"),header = True)

# COMMAND ----------


