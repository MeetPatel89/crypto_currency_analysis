# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake Containers using Service Principal
# MAGIC
# MAGIC ##### Steps to follow
# MAGIC 1. Get client id, tenant id and client secret from key vault
# MAGIC 2. Set Spark Config with App/Client Id, Directory/Tenant Id and Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

def mount_adls(storage_account, container):
    # Get secrets from key vault
    client_id = dbutils.secrets.get(scope="crypto-analysis-scope",key="cryptoClientId")
    tenant_id = dbutils.secrets.get(scope="crypto-analysis-scope",key="cryptoTenantId")
    service_credential = dbutils.secrets.get(scope="crypto-analysis-scope",key="cryptoClientSecret")

    # Set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": service_credential,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    potential_mount = f"/mnt/{storage_account}/{container}"
    
    # unmount if mount is already existent
    if any(potential_mount == mount.mountPoint for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(potential_mount)

    # mount the storage account container onto DBFS
    dbutils.fs.mount(
        source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
        mount_point = potential_mount,
        extra_configs = configs)
    
    # display mounts
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Raw Container

# COMMAND ----------

mount_adls("cryptoanalysisdl", "raw")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Processed Container

# COMMAND ----------

mount_adls("cryptoanalysisdl", "processed")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount Presentation Container

# COMMAND ----------

mount_adls("cryptoanalysisdl", "presentation")
