-- Databricks notebook source
SHOW STORAGE CREDENTIALS;

-- COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.datalakeanki1.dfs.core.windows.net",
  "neA6B/tyKMdqvDLMtlAcXpE0w+FRUKgamxPmxcx/NjPkAkf3uo72EOBagkRmRKYKxQJWeXTL9QnL+AStJCI5Pw=="
)

-- COMMAND ----------

SHOW STORAGE CREDENTIALS;

-- COMMAND ----------

CREATE STORAGE CREDENTIAL;
CREATE EXTERNAL LOCATION;