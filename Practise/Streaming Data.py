# Databricks notebook source
# MAGIC %md
# MAGIC #AUTO LOADER

# COMMAND ----------

# MAGIC %md
# MAGIC **Stream Loading**

# COMMAND ----------


df=spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", "abfss://aldestination@datalakeanki1.dfs.core.windows.net/checkpoint")\
            .load("abfss://alsource@datalakeanki1.dfs.core.windows.net")


# COMMAND ----------

df.writeStream.format("delta").option("checkpointLocation", "abfss://aldestination@datalakeanki1.dfs.core.windows.net/checkpoint")\
.trigger(processingTime='10 seconds')\
.start("abfss://aldestination@datalakeanki1.dfs.core.windows.net/data")

# COMMAND ----------

# MAGIC %md
# MAGIC