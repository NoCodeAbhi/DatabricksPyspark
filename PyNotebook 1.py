# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading
# MAGIC

# COMMAND ----------

dbutils.fs.ls('FileStore/tables')

# COMMAND ----------

df = spark.read.format('csv').option('Inferschema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Reading JSON

# COMMAND ----------

dbutils.fs.ls('FileStore/tables')

# COMMAND ----------

df_json = spark.read.format('json').option('Inferschema',True)\
                    .option('header',True)\
                    .option('multiline',False)\
                    .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema = '''
                Item_Identifier string,
                Item_Weight integer ,
                Item_Fat_Content string,
                Item_Visibility double,
                Item_Type string,
                Item_MRP double,
                Outlet_Identifier string,
                Outlet_Establishment_Year integer,
                Outlet_Size string ,
                Outlet_Location_Type string,
                Outlet_Type string,
                Item_Outlet_Sales double
                '''
df1 = spark.read.format('csv').schema(my_ddl_schema)\
                .option('header',True)\
                .load('/FileStore/tables/BigMart_Sales.csv')


# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###StructType()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SELECT transformation

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

#1st approach
#df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

#2nd approach
df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###FILTER/WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario-1

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario -2

# COMMAND ----------

df.filter((col('Item_Weight')<=10) & (col('Item_Type')=='Soft Drinks')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario - 3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ###withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_W').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario -1 (To add new column with constant value)

# COMMAND ----------

#lit()--> constatnt value, flag--> new column function
df.withColumn('flag',lit('new')).display()

# COMMAND ----------

#Multiply two columns and store results in new column--> Multiply is new column
df.withColumn('Multiply',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario - 2 (Replace column Content)

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace('Item_Fat_Content',"Regular","Reg"))\
.withColumn('Item_Fat_Content',regexp_replace('Item_Fat_Content',"Low Fat","LF")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###TypeCasting

# COMMAND ----------

df = df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))



# COMMAND ----------

df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Sort/OrderBy

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario - 1

# COMMAND ----------

#Sort column in desc order
df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

#Sort column in asc order
df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario - 2

# COMMAND ----------

#sort two columns, both in desc order. 0 --> False, asc = False means desc
df.sort('Item_Weight','Item_Visibility',ascending = [0,0]).display()

# COMMAND ----------

#sort two column, one in asc and other desc
df.sort('Item_Weight','Item_Visibility',ascending = [1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DROP

# COMMAND ----------

#Scenario - 1(Drop single column)
df.drop('Item_Visibility').display()

# COMMAND ----------

#Scenarion - 2 (Drop multiple column)
df.drop('Item_Weight','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop Duplicates (also called DDup)

# COMMAND ----------

df.display()

# COMMAND ----------

#Drop duplicated base on all the columns
df.drop_duplicates().display()


# COMMAND ----------

#if dont want to use drop_duplicates
df.distinct().display()

# COMMAND ----------


#drop duplicates based on specific columns. Can provide multiple columns as list.
df.dropDuplicates(subset=['Item_type']).display()
df.dropDuplicates(subset=['Item_type','Item_Weight']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Union and Union BYNAME

# COMMAND ----------

# MAGIC %md
# MAGIC ###UNION

# COMMAND ----------

data1 = [(1,'Abhi'),(2,'Shek')]
schema1 = 'id Integer, Name STRING'
df1 = spark.createDataFrame(data1,schema1)
data2 = [('3','pan'),('4','Dey')]
schema2 = 'id STRING, Name STRING'
df2 = spark.createDataFrame(data2,schema2)
df1.display()
df2.display()

# COMMAND ----------

#UNION
df1.union(df2).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###UNION BYNAME

# COMMAND ----------

data1 = [('1','Abhi'),('2','Shek')]
schema1 = 'id STRING, Name STRING'
df1 = spark.createDataFrame(data1,schema1)
data2 = [('pan','3'),('Dey','4')]
schema2 = 'Name STRING, id STRING'
df2 = spark.createDataFrame(data2,schema2)


# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

#If i do union now
df1.union(df2).display()

# COMMAND ----------

#UNION BYNAME
df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###STRING Functions

# COMMAND ----------

dbutils.fs.ls('FileStore')

# COMMAND ----------

df = spark.read.format('csv')\
            .option('Inferschema',True)\
            .option('header',True)\
                .load('/FileStore/tables/BigMart_Sales.csv')
df.display()

# COMMAND ----------

#initcap() to capital first letter
df.select(initcap('Item_Type')).display()

# COMMAND ----------

#upper() to make all in capital letter
df.select(upper('Item_Type')).display()

# COMMAND ----------

#lower() to make all lower
df.select(lower('Item_Type').alias('Lower_Item')).display() #can use alias as well

# COMMAND ----------

# MAGIC %md
# MAGIC ###Date Functions

# COMMAND ----------

#Current_date()
df = df.withColumn('Current Date',current_date())
df.display()

# COMMAND ----------

#date_add() function to add future date in a colum
df = df.withColumn('Week_after',date_add('Current Date',7))
df.display()


# COMMAND ----------

#date_sub() to subtract date 
#df.withColumn('Week_before',date_sub('current date',7)).display()

# COMMAND ----------

#we can achieve same result as date_sub() using date_add() by adding negative value
df = df.withColumn('week_before',date_add('current date',-7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATEDIFF

# COMMAND ----------

df = df.withColumn('date_diff',datediff('week_after','current date'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATE_FORMAT

# COMMAND ----------

df = df.withColumn('week_after',date_format('week_after','dd-mm-yyyy'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handling NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ####Dropping Nulls
# MAGIC

# COMMAND ----------

#dropna(all)--> all means drop from all the columns where have nulls in same row
df.dropna('all').display()

# COMMAND ----------

#dropna(any)--> drop null from any columnes wherever present
df.dropna('any').display()

# COMMAND ----------

#use subset to delete null from specific column
df.dropna(subset = ['Outlet_Size','Item_Weight']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Fill Null

# COMMAND ----------

df.fillna('Not Available').display()

# COMMAND ----------

#Using subset
df.fillna('Not Available',subset=['Outlet_Size']).display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn('Item_weight',col('Item_Weight').cast(StringType()))
df.display()

# COMMAND ----------

df.fillna('Not Available',subset = ['Item_Weight']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Split and Indexing
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####SPLIT

# COMMAND ----------

df.withColumn('Outlet_Type',split('outlet_type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Indexing

# COMMAND ----------

#we can use index number to return that value in column, here i have used [1] or [0]
df.withColumn('Outlet_Type',split('outlet_type',' ')[0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLOD - to store the splitted value in seperate rows

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Array_contains()

# COMMAND ----------

#array_contains() helps us to check if particular entitiy of array present or not, return boolean value
df_exp.withColumn('Type_1_flag',array_contains('outlet_type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Group BY

# COMMAND ----------

df_grp = df.select(col('Item_Type'),col('Item_MRP'),col('Outlet_Identifier'))
df_grp.display()

# COMMAND ----------

#use of group by and sum()
df_grp.groupby('Item_Type').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

#find the avg() of Item_MRP
df_grp.groupby('Item_Type').agg(avg('Item_MRP').alias('Avg_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Group by on multiple columns

# COMMAND ----------

df.display()

# COMMAND ----------

df_grp2 = df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP'))
df_grp2.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 2

# COMMAND ----------

df.groupby('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP'),avg('Item_MRP').alias('Avg_MRP'),count('Item_Type').alias('Total_Count')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Collect_list

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book3'),
        ('user3','book1')]
schema = 'user STRING,books STRING'
new_df = spark.createDataFrame(data,schema)
new_df.display()

# COMMAND ----------

#lets use collect_list()--> It creates the list of value for same key
new_df.groupby('user').agg(collect_list('books')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###PIVOT

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Item_Type').pivot('outlet_size').agg(avg('Item_MRP').alias('Avg_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###WHEN-OTHERWISE

# COMMAND ----------

#if we want to aplly condition on a particular column if value is this then do this

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario -1

# COMMAND ----------

df= df.withColumn('Veg_Flag',when(col('Item_Type') == 'Meat','Non-Veg').otherwise('Veg'))
df.display()

# COMMAND ----------

df.select('Item_Type','Veg_Flag').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 2

# COMMAND ----------

#multiple when conditions with multiple value
df.withColumn('Veg_expense',when(((col('Veg_Flag')=='Veg') & (col('Item_MRP')<100)),'Veg_Inexpensive')\
                            .when((col('Veg_Flag')=='Veg') & (col('Item_MRP')>100),'Veg_Expensive')\
                                .otherwise('Non_Veg')).display() 

# COMMAND ----------

df.withColumn('Diet',when(col('Item_Fat_Content')=='Low Fat','Good').otherwise('Bad')).display()

# COMMAND ----------

