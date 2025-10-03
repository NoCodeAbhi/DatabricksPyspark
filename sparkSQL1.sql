-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Python part

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Check file location
-- MAGIC dbutils.fs.ls('FileStore/sparkSQL')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format('csv').option('Inferschema',True).option('header',True).load('/FileStore/sparkSQL/ecommerce_orders_large.csv')
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Spark SQL basics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Temp View
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView('order_temp')

-- COMMAND ----------

select * from order_temp;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Global Temp Views

-- COMMAND ----------

-- #Don't run in serverless compute
-- df.createOrReplaceGlobalTempView('order_global_temp')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##External VS Managed Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Managed Table

-- COMMAND ----------

-- First create catalog
CREATE CATALOG sparksql_cata

-- COMMAND ----------

-- Then create schema followed by catalog name
CREATE SCHEMA sparksql_cata.sparksql_schema

-- COMMAND ----------

-- then create table it can be managed or external. Table uses 3 level name space
CREATE TABLE sparksql_cata.sparksql_schema.order_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###External Table

-- COMMAND ----------

CREATE TABLE sparksql_cata.sparksql_schema.order_ext
location "adfss/path where you want to store data"
AS 
SELECT from order_temp;
