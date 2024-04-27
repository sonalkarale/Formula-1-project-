# Databricks notebook source
# MAGIC %md
# MAGIC Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope ='formula1-secret-scope')

# COMMAND ----------



# COMMAND ----------


