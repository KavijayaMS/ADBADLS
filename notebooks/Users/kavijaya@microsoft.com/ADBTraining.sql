-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.secrets.get(scope="ADBADLSScope", key="dlstoken")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val configs = Map(
-- MAGIC   "fs.azure.account.auth.type" -> "OAuth",
-- MAGIC   "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
-- MAGIC   "fs.azure.account.oauth2.client.id" -> "73b7b0b7-9f6d-49fa-b332-aa1457e511f6",
-- MAGIC   "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope="ADBADLSScope", key="dlstoken"),
-- MAGIC   "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = "abfss://data@rachitstoragetraining.dfs.core.windows.net/",
-- MAGIC   mountPoint = "/mnt/data",
-- MAGIC   extraConfigs = configs)

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC 
-- MAGIC ls /mnt/data 

-- COMMAND ----------

