-- Databricks notebook source
-- MAGIC %python
-- MAGIC print("Hello Sir")

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC # Databrick MasterClass
-- MAGIC **Data Reading**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC mydata=[(1,'aa',30),(2,'bb',40),(3,'cc',50),(4,'dd',30)]
-- MAGIC myschema="Id int,name string,Marks int"
-- MAGIC df=spark.createDataFrame(mydata,schema=myschema)
-- MAGIC display(df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Access Data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #application_id=83fdee67-6006-4f0d-b8a8-d85a8876abc6
-- MAGIC #object_d=69afcb26-54e2-47f1-9916-868daaebf042
-- MAGIC #directory tenant id=875c4e56-bf67-432b-a849-1375da936ad6
-- MAGIC #secret=""
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.unset("fs.azure.account.key.datalakeanki1.dfs.core.windows.net")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC configs_to_clear = [
-- MAGIC     "fs.azure.account.key.datalakeanki1.dfs.core.windows.net",
-- MAGIC     "fs.azure.account.auth.type.datalakeanki1.dfs.core.windows.net",
-- MAGIC     "fs.azure.account.oauth.provider.type.datalakeanki1.dfs.core.windows.net",
-- MAGIC     "fs.azure.account.oauth2.client.id.datalakeanki1.dfs.core.windows.net",
-- MAGIC     "fs.azure.account.oauth2.client.secret.datalakeanki1.dfs.core.windows.net",
-- MAGIC     "fs.azure.account.oauth2.client.endpoint.datalakeanki1.dfs.core.windows.net"
-- MAGIC ]
-- MAGIC
-- MAGIC for c in configs_to_clear:
-- MAGIC     try:
-- MAGIC         spark.conf.unset(c)
-- MAGIC     except:
-- MAGIC         pass

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC storage_account = "datalakeanki1"
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC     f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
-- MAGIC     "OAuth"
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC     f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
-- MAGIC     "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC     f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
-- MAGIC     "83fdee67-6006-4f0d-b8a8-d85a8876abc6"
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC     f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
-- MAGIC     ""
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC     f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
-- MAGIC     "https://login.microsoftonline.com/875c4e56-bf67-432b-a849-1375da936ad6/oauth2/token"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Db Utilities
-- MAGIC **dbutils.fs**
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls(
-- MAGIC         "abfss://source@datalakeanki1.dfs.core.windows.net/"
-- MAGIC     )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **dbutils.widgets**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("p_name","singh")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC var=dbutils.widgets.get("p_name")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC var

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **dbutils.secrets**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.secrets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.secrets.list(scope='ankiscope')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.secrets.get(scope='ankiscope',key='app-secret')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales=spark.read.format('csv').option('header',True)\
-- MAGIC                        .option('inferSchema',True)\
-- MAGIC                        .load ("abfss://source@datalakeanki1.dfs.core.windows.net/")    

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df_sales)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.withColumn('Item_types',split(col('Item_type'),' ')).display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_sales.withColumn('Item_Visibility',col('Item_Visibility').cast(StringType())).display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Delta Lake

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df_sales.write.format('delta')\
-- MAGIC                .mode('append')\
-- MAGIC                .option('path','abfss://destination@datalakeanki1.dfs.core.windows.net/sales')\
-- MAGIC                .save()  
-- MAGIC      
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Managed Table

-- COMMAND ----------

CREATE DATABASE sales_db;

-- COMMAND ----------

Create Table sales_db.mantable(
    Id int,
    name string,
    Marks int
)
USING DELTA

-- COMMAND ----------

Insert into sales_db.mantable VALUES (1,'aa',30), (2,'bb',40), (3,'cc',50), (4,'dd',30);

-- COMMAND ----------

Select * from sales_db.mantable;

-- COMMAND ----------

DROP TABLE sales_db.mantable;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #External Table

-- COMMAND ----------


SELECT current_metastore();

-- COMMAND ----------


CREATE STORAGE CREDENTIAL my_cred
WITH AZURE_SERVICE_PRINCIPAL (
  CLIENT_ID '83fdee67-6006-4f0d-b8a8-d85a8876abc6',
  CLIENT_SECRET '',
  TENANT_ID '875c4e56-bf67-432b-a849-1375da936ad6'
);

-- COMMAND ----------

Create Table sales_db.exttable(
    Id int,
    name string,
    Marks int
)
USING DELTA
LOCATION 'abfss://destination@datalakeanki1.dfs.core.windows.net/salesDB/'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(
-- MAGIC   "fs.azure.account.auth.type.datalakeanki1.dfs.core.windows.net",
-- MAGIC   "OAuth"
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC   "fs.azure.account.oauth.provider.type.datalakeanki1.dfs.core.windows.net",
-- MAGIC   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC   "fs.azure.account.oauth2.client.id.datalakeanki1.dfs.core.windows.net",
-- MAGIC   "83fdee67-6006-4f0d-b8a8-d85a8876abc6"
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC   "fs.azure.account.oauth2.client.secret.datalakeanki1.dfs.core.windows.net",
-- MAGIC   "ueP8Q~KpvlIdO0MNeZ1HRJ1am_nJX2SeC1kHidqW"
-- MAGIC )
-- MAGIC
-- MAGIC spark.conf.set(
-- MAGIC   "fs.azure.account.oauth2.client.endpoint.datalakeanki1.dfs.core.windows.net",
-- MAGIC   "https://login.microsoftonline.com/875c4e56-bf67-432b-a849-1375da936ad6/oauth2/token"
-- MAGIC )

-- COMMAND ----------


CREATE SCHEMA IF NOT EXISTS sales_db;

CREATE TABLE sales_db.exttable(
    Id INT,
    name STRING,
    Marks INT
)
USING DELTA
LOCATION 'abfss://destination@datalakeanki1.dfs.core.windows.net/salesDB/exttable';