# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC Step to follow
# MAGIC
# MAGIC 1.Get Client ID, tenant_id, and Client_secret from Key vault
# MAGIC 2. Set spark config with App/client id ,Directory/Tenanat id and Secret
# MAGIC 3. Call file System utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts,unmount).
# MAGIC

# COMMAND ----------

storage_account_name = 'formula1datalake24'
client_id =dbutils.secrets.get(scope ='formula1-secret-scope',key ='formula1-client-id')
tenant_id =dbutils.secrets.get(scope ='formula1-secret-scope',key ='formula1-tenant-ID')
client_secret = dbutils.secrets.get(scope ='formula1-secret-scope',key ='formula1-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point= f"/mnt/{storage_account_name}/{container_name}",
        extra_configs=configs)

# COMMAND ----------

mount_adls("demo1")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1datalake24/demo1")

# COMMAND ----------

# dbutils.fs.mount(
#   source = "abfss://demo@formula1datalake24.dfs.core.windows.net/",
#   mount_point = "/mnt/formula1datalake24/demo",
#   extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1datalake24/demo1"))

# COMMAND ----------


