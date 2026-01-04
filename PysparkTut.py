# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading JSON

# COMMAND ----------

dbutils.fs.ls('/Volumes/workspace/pysparkcsv/sparkjson')

# COMMAND ----------

df_json = spark.read.format('json').option('inferSchema',True)\
    .option('header', True)\
    .option('multiline', False)\
    .load('/Volumes/workspace/pysparkcsv/sparkjson/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data Reading 
# MAGIC

# COMMAND ----------

dbutils.fs.ls('/Volumes/workspace/pysparkcsv/csvfile')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/Volumes/workspace/pysparkcsv/csvfile/BigMart Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Defination
# MAGIC

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL SCHEMA

# COMMAND ----------

my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING,
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INTEGER,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING,
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE
                  '''




# COMMAND ----------

df = spark.read.format('csv')\
                .schema(my_ddl_schema)\
                .option('header',True)\
                .load('/Volumes/workspace/pysparkcsv/csvfile/BigMart Sales.csv')



# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_strct_schema = StructType([
                                StructField('Item_Identifier',StringType(),True),
                                StructField('Item_Weight',StringType(),True),
                                StructField('Item_Fat_Content',StringType(),True),
                                StructField('Item_Visibility',StringType(),True),
                                StructField('Item_Type',StringType(),True),
                                StructField('Item_MRP',StringType(),True),
                                StructField('Outlet_Identifier',StringType(),True),
                                StructField('Outlet_Establishment_Year',StringType(),True),
                                StructField('Outlet_Size',StringType(),True),
                                StructField('Outlet_Location_Type',StringType(),True),
                                StructField('Outlet_Type',StringType(),True),
                                StructField('Item_Outlet_Sales',StringType(),True)


])

# COMMAND ----------

df = spark.read.format('csv')\
                .schema(my_strct_schema)\
                .option('header',True)\
                .load('/Volumes/workspace/pysparkcsv/csvfile/BigMart Sales.csv')


# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT

# COMMAND ----------

df.display()

# COMMAND ----------

df_sel = df.select('Item_Identifier','Item_Weight','Item_Fat_Content')

# COMMAND ----------

df_sel.display()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS = to change the column name

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 1 = Filter the data with fat content = Regular

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 2 = Slice the data with item type = Soft Drinks and weight <=10

# COMMAND ----------

df.filter(
    (col("Item_Type") == "Soft Drinks") &
    (col("Item_Weight").cast("double") <= 10)
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 3 = Fetch the data with Tier in (Tier1 or Tier 2) and Outlet size is Null
# MAGIC ### .isin() and .isNull()

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter( (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2')) & (col('Outlet_Size').isNull()) ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','weight').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withcolumn = create new column

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1 = Creating a new column

# COMMAND ----------

# MAGIC %md
# MAGIC ### lit = function that avaiable to create a new column with a constant value

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df = df.withColumn('flag', lit('new'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('product_item', col('Item_Weight').cast('double') * col('Item_MRP').cast('double')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario - 2 = regexp_replace()
# MAGIC
# MAGIC **Definition:**  
# MAGIC `regexp_replace()` is a function in PySpark that replaces all substrings of a string that match a regular expression pattern with a specified replacement string.
# MAGIC
# MAGIC **How it works:**  
# MAGIC It takes three arguments: the column to operate on, the regex pattern to search for, and the replacement string. For every match of the pattern in the column, it replaces the matched substring with the replacement string.
# MAGIC
# MAGIC **Example:**  
# MAGIC python
# MAGIC from pyspark.sql.functions import regexp_replace
# MAGIC df.withColumn('new_column', regexp_replace(col('old_column'), 'pattern', 'replacement'))

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

df = df.withColumn(
    'Item_Fat_Content',
    regexp_replace(col('Item_Fat_Content'), 'Low Fat', 'LF')
).withColumn(
    'Item_Fat_Content',
    regexp_replace(col('Item_Fat_Content'), 'Regular', 'R')
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Type Casting
# MAGIC
# MAGIC **Definition:**  
# MAGIC Type casting in PySpark refers to converting the data type of a column to another data type using the `cast()` function. This is useful when you need to change the type of data for analysis or processing, such as converting a string column to integer or float.
# MAGIC
# MAGIC **Example:**  
# MAGIC python
# MAGIC ### from pyspark.sql.functions import col
# MAGIC df.withColumn('column_name', col('column_name').cast('Integer'))

# COMMAND ----------

from pyspark.sql.types import StringType

df = df.withColumn(
    'Item_weight',
    col('Item_Weight').cast(StringType())
)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort/OrderBy
# MAGIC
# MAGIC **Explanation:**  
# MAGIC Sorting or ordering data means arranging the rows of a DataFrame based on the values of one or more columns. In PySpark, you can use the `orderBy()` or `sort()` function to sort the data in ascending or descending order.
# MAGIC
# MAGIC **Example:**  
# MAGIC python
# MAGIC ### # df.orderBy('column_name').display()  # Sorts by column_name in ascending order
# MAGIC ### # df.orderBy(col('column_name').desc()).display()  # Sorts by column_name in descending order

# COMMAND ----------

df.orderBy('Item_Weight').display()

# COMMAND ----------

df.orderBy(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort('Item_Visibility').display()

# COMMAND ----------

df.sort(col('Item_Visibility').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort with multiple columns

# COMMAND ----------

df.sort(col('Item_Weight'), col('Item_Visibility'),ascending = [0,0]).display()

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limit: Restricts the number of rows returned from a DataFrame.
# MAGIC ### Example: df.limit(5) returns only the first 5 rows.

# COMMAND ----------


df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop - used to drop the column

# COMMAND ----------

df.drop(col('Item_Visibility')).display()

# COMMAND ----------

df.drop('Outlet_Establishment_Year','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop_Duplicates
# MAGIC
# MAGIC **Definition:**  
# MAGIC `dropDuplicates()` is a PySpark DataFrame function that removes duplicate rows based on one or more specified columns. It returns a new DataFrame with only unique rows, helping to ensure data quality by eliminating repeated records.
# MAGIC
# MAGIC **Example:**  
# MAGIC python
# MAGIC ### df.dropDuplicates(['column1', 'column2'])

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.dropDuplicates(subset=['Item_Type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### distinct = drop based on column

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union
# MAGIC
# MAGIC **Definition:**  
# MAGIC `union()` is a PySpark DataFrame function that combines the rows of two DataFrames with the same schema into a single DataFrame. The columns must be in the same order and have the same names and data types.
# MAGIC
# MAGIC **Example:**  
# MAGIC python
# MAGIC ### df1.union(df2)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Union By Name
# MAGIC
# MAGIC **Definition:**  
# MAGIC `unionByName()` is a PySpark DataFrame function that combines the rows of two DataFrames by matching column names, regardless of their order. This is useful when the column order differs between DataFrames but the names and data types match.
# MAGIC
# MAGIC **Example:**  
# MAGIC python
# MAGIC ### df1.unionByName(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing Dataframes

# COMMAND ----------

data1 = [('1', 'kad'),
        ('2', 'sid')]
schema1 = 'id STRING , name STRING'
df1 = spark.createDataFrame(data1, schema1)

data2 = [('3', 'Rahul'),
        ('4', 'ujjwal')]
schema2 = 'id STRING , name STRING'
df2 = spark.createDataFrame(data2, schema2)

df1.union(df2).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### UnionByName

# COMMAND ----------

data1 = [('kad','1'),
        ('sid', '2')]
schema1 = 'id STRING , name STRING'
df1 = spark.createDataFrame(data1, schema1)
df1.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### # String Functions Definitions:
# MAGIC ### # initcap(): Converts the first character of each word in a string to uppercase and the rest to lowercase.
# MAGIC ### # upper(): Converts all characters in a string to uppercase.
# MAGIC ### # lower(): Converts all characters in a string to lowercase.
# MAGIC

# COMMAND ----------

df.display()

# COMMAND ----------

### Initcap()
df.select(initcap('Item_Type')).display()

# COMMAND ----------

df.select(lower('Item_Type')).display()

# COMMAND ----------

df.select(upper('Item_Type').alias('Upper_Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Functions
# MAGIC
# MAGIC **current_date():**  
# MAGIC Returns the current date of the system in the format `yyyy-MM-dd`.
# MAGIC
# MAGIC **date_add(start_date, num_days):**  
# MAGIC Adds a specified number of days (`num_days`) to the given date (`start_date`). Returns the new date.
# MAGIC
# MAGIC **date_sub(start_date, num_days):**  
# MAGIC Subtracts a specified number of days (`num_days`) from the given date (`start_date`). Returns the new date.
# MAGIC
# MAGIC **Example Usage:**
# MAGIC python
# MAGIC from pyspark.sql.functions import current_date, date_add, date_sub
# MAGIC
# MAGIC df.select(current_date().alias('Today')).display()
# MAGIC df.select(date_add(current_date(), 5).alias('5_days_later')).display()
# MAGIC df.select(date_sub(current_date(), 5).alias('5_days_ago')).display()

# COMMAND ----------

# Current Date
df = df.withColumn('curr_date', current_date())
df.display()

# COMMAND ----------

# Date_add()
df = df.withColumn('week_after', date_add('curr_date',7))
df.display()

# COMMAND ----------

# Date_sub
df = df.withColumn('week_before', date_sub('curr_date',5))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DateDiff

# COMMAND ----------

df = df.withColumn ('datediff', datediff('week_after','curr_date'))
df.display()

# COMMAND ----------

df = df.withColumn ('date_diff', datediff('week_before','week_after'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date_Format

# COMMAND ----------

df = df.withColumn('week_before',date_format('week_before','dd-mm-yyyy'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Handling Nulls = dropping and filling nulls

# COMMAND ----------

# dropping nulls
df = df.dropna()
df.display()

# COMMAND ----------

# filling nulls
df = df.fillna(0)
df.display()

# COMMAND ----------

# Using Subset
df.dropna(subset= 'Outlet_Type').display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####  Split and Indexing
# MAGIC
# MAGIC ## # split(): Splits a string into an array using a specified delimiter.
# MAGIC ## # getItem(): Retrieves an element from an array at a specified index.
# MAGIC
# MAGIC ### from pyspark.sql.functions import split
# MAGIC ### 
# MAGIC ### df = df.withColumn('Item_Type_Split', split('Item_Type', ' '))
# MAGIC ### df = df.withColumn('First_Word', df['Item_Type_Split'].getItem(0))
# MAGIC ### df.display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Group_BY

# COMMAND ----------

# Scenario 1
df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

# Scenario 2
df.groupBy('Item_Type').agg(avg("Item_MRP")).display()

# COMMAND ----------

# Scenario 3
df.groupBy('Item_Type', 'Outlet_Size').agg(sum('Item_MRP')).alias('total_mrp').display()


# COMMAND ----------

# Scenario 4
df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('total_mrp'),avg('Item_MRP').alias('avg_mrp')).display()

# COMMAND ----------

