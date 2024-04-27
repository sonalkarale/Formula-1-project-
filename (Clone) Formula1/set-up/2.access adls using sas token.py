# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using SAS token
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. LIst files from demo container
# MAGIC 3. Read data from ciruit.csv file
# MAGIC

# COMMAND ----------

formula1_demo_SAS_token =dbutils.secrets.get(scope ='formula1-secret-scope',key='formula1-demo-SAS-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1datalake24.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1datalake24.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1datalake24.dfs.core.windows.net",formula1_demo_SAS_token)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1datalake24.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1datalake24.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake24.dfs.core.windows.net/002 circuits.csv"))

# COMMAND ----------


