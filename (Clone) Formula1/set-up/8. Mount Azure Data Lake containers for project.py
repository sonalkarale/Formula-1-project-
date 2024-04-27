# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake Containers for Project 
# MAGIC
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name,container_name):
    # Get Secret from key vault
    client_id =dbutils.secrets.get(scope ='formula1-secret-scope',key ='formula1-client-id')
    tenant_id =dbutils.secrets.get(scope ='formula1-secret-scope',key ='formula1-tenant-ID')
    client_secret = dbutils.secrets.get(scope ='formula1-secret-scope',key ='formula1-client-secret')

    # Set Spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # unmount the mount point if its already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    
    # Mount the storage account Container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())
        

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount raw container 

# COMMAND ----------

mount_adls('formula1datalake24','raw')

# COMMAND ----------

mount_adls('formula1datalake24','processed')

# COMMAND ----------

mount_adls('formula1datalake24','presentation')

# COMMAND ----------


