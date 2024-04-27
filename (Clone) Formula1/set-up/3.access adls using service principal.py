# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake Using Service Principal
# MAGIC Step to follow
# MAGIC 1. Register Azure AD Application/service principal
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set Spark Config with App/client id,Directory/Tenant id and Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data lake.
# MAGIC

# COMMAND ----------

client_id =dbutils.secrets.get(scope ='formula1-secret-scope',key ='formula1-client-id')
tenant_id =dbutils.secrets.get(scope ='formula1-secret-scope',key ='formula1-tenant-ID')
client_secret = dbutils.secrets.get(scope ='formula1-secret-scope',key ='formula1-client-secret')

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.formula1datalake24.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1datalake24.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1datalake24.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1datalake24.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1datalake24.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1datalake24.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1datalake24.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1datalake24.dfs.core.windows.net/002 circuits.csv"),header = True)

# COMMAND ----------


