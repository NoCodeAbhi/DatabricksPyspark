-- Databricks notebook source
SELECT * from sparksql_cata.sparksql_schema.order_ext -- it works only if you have registered unity catalog

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format('csv').option('Inferschema',True).option('header',True).load('/FileStore/sparkSQL/ecommerce_orders_large.csv')
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Temp View to use sql query as we are not using unity catalog so cant access multiple workspace

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView('orders_temp')

-- COMMAND ----------

select * from orders_temp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Filtering the data
-- MAGIC

-- COMMAND ----------

select * from orders_temp
where product_category = 'Fashion'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #TO convert this rersult into dataframe use spark.sql
-- MAGIC sql_to_df = spark.sql('''select * from orders_temp
-- MAGIC where product_category = "Fashion";''')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_to_df.display()

-- COMMAND ----------

