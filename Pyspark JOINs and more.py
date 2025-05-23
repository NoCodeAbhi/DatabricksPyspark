# Databricks notebook source
# MAGIC %md
# MAGIC ###Inner Join

# COMMAND ----------

emp = [('1','Abhishek','d01'),
        ('2','Abhi','d02'),
        ('3','Aman','d03'),
        ('4','Ajay','d03'),
        ('5','Kavi','d05'),
        ('6','Vaka','d06')]
emp_schema = 'Id STRING, Name STRING, dept_id STRING'
df1 = spark.createDataFrame(emp, emp_schema)
dep = [('d01','HR'),
       ('d02','Marketing'),
       ('d03','Accounts'),
       ('d04','IT'),
       ('d05','Finance')]
dep_schema = 'dept_id STRING, department STRING'
df2 = spark.createDataFrame(dep, dep_schema)



# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

#use Inner join
df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],how='inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Left Join

# COMMAND ----------

##return all value from left table and matched column from right table, for non matched value returns null. if any unique columne value present in right table but not in left table, skips that value

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Right JOin

# COMMAND ----------

#return all value from right table and matched column from left table, for non matched value returns null. if any unique columne value present in left table but not in right table, skips that value
df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Full Join

# COMMAND ----------

#return all the value from both the tables, for non matched value retuen null
df1.join(df2,df1['dept_id']==df2['dept_id'], how = 'full').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Anti Join

# COMMAND ----------

#To fetch the only records with are not in another table

df1.join(df2, df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Window Function

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format('csv').option('Inferschema',True)\
            .option('Header',True)\
                .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Row Number() Function

# COMMAND ----------

df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rank() and Dense_rank()
# MAGIC

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy('Item_Identifier')))\
    .withColumn('dense_rank',dense_rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

#use of acs and desc with rank function
df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Cumulative Function in Window Function - .rowsBetween()

# COMMAND ----------


#before .rowsBetween() it will return the total sum of particular item type the calculate the total some of other item type.
df.withColumn('CumSum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

#PractiCe
df.withColumn('CumSum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.withColumn('TotalSum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writing Data

# COMMAND ----------

# MAGIC %md
# MAGIC ###CSV

# COMMAND ----------

df.write.format('csv').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

#another way to save file
df.write.format('csv').option('Path','/FileStore/tables/CSV/data1.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####writing mode
# MAGIC - Append
# MAGIC - owerwrite
# MAGIC - Error
# MAGIC - Ignore

# COMMAND ----------

df.write.format('csv').mode('append').save('/FileStore/tables/CSV/data1.csv')

# COMMAND ----------

df.write.format('csv').mode('overwrite').save('/FileStore/tables/CSV/data1.csv')

# COMMAND ----------

df.write.format('csv').mode('Error').save('/FileStore/tables/CSV/data1.csv')

# COMMAND ----------

df.write.format('csv').mode('ignore').save('/FileStore/tables/CSV/data1.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Parquet file format

# COMMAND ----------

#parquet file is columner based file format which is mostlly used for big data for better performance and storage.

df.write.format('parquet')\
    .option('Path','/FIleStore/tables/CSV/data.csv')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables

# COMMAND ----------

#create table instead oof writing in file
df.write.format('parquet')\
        .saveAsTable('my_table')

# COMMAND ----------

#Managed Table - We create table but its data will aslo store at some location that will be stored in databricks(datalake) location. if we drop table, data will also delete.
# External Table - We create table but we store that data at our specified location so if we want to drop table, the data will not going to be delete. 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ####CreateTempView

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from my_view
# MAGIC where item_fat_content = 'LF'

# COMMAND ----------

#to write sql in df, use spark.sql func()
df_sql = spark.sql("select * from my_view where item_fat_content = 'LF' ")

# COMMAND ----------

df_sql.display()

# COMMAND ----------

df = spark.read.format('csv').option('Inferschema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')
df.display()

# COMMAND ----------

df = df.distinct()
df.display()

# COMMAND ----------

df = df.filter(col('Item_Weight')>15)
df.display()

# COMMAND ----------

df.groupBy('department').agg(avg('salary').alias('Avg_alary'))

# COMMAND ----------

df2 = df1.withColumn('FullName',split('FullName',' '))\
        .withColumn('FirstName',col('FullName')[0])\
        .withColumn('LastName',col('FullName')[1])
df2.display()

# COMMAND ----------

from pyspark.sql.functions import *
data = [
    {
        'id':1,
        'name':'Abhishek',
        'Contacts': [
            {'type':'email','value':'abhi@gmail.com'},
            {'type':'mobile','value':'123455'}
        ]
    }
]

df = spark.createDataFrame(data)
flatten_df = df.withColumn('Contacts',explode('contacts'))\
                .select('id','name',col('contacts.type').alias('contact_type'),col('contacts.value').alias('contact_value'))
                    
flatten_df2 = flatten_df.groupBy('name').pivot('contact_type').agg(first('contact_value'))
flatten_df2.display()

# COMMAND ----------

df2 = flatten_df2.withColumn("age", floor(rand() * 40 + 20))
df2.display()

# COMMAND ----------

df2.withColumn('Status',when(col('age')>18,'Adult').otherwise('Minor')).display()

joined_df = emp.join(dept,emp['dept_id']==dept['dept_id'],'inner')
joined_df.fileter(col('salary')>50000)

# COMMAND ----------

#row_number()
row_df = df.withColumn('row',row_number().over(Window.partitionBy('department'), Window.orderBy(desc('Salary'))))
salary_df = row_df.filter(col(row)<=3)