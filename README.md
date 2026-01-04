📌 PySpark DataFrame Operations – Learning Notebook
📖 Overview

This repository contains my hands-on PySpark learning notebook, where I explored and practiced core PySpark DataFrame operations using Databricks.
The notebook covers data reading, schema handling, transformations, filtering, aggregations, string & date functions, and more.

This project helped me build a strong foundation in big data processing using Apache Spark (PySpark).

🛠️ Technologies Used

Apache Spark (PySpark)

Databricks Notebook

Python

CSV & JSON Data Sources

📂 Data Sources

JSON File

Used to understand JSON reading and schema inference

CSV File (BigMart Sales Dataset)

Used for most DataFrame operations and transformations

🚀 Concepts Covered
🔹 Data Reading

Reading JSON files

Reading CSV files

Using inferSchema

Handling headers

Working with Databricks file system (dbutils.fs.ls)

🔹 Schema Management

Auto-inferred schema

DDL Schema definition

StructType & StructField schema

Schema comparison and validation

🔹 DataFrame Operations

select() with columns

Column alias using alias()

filter() with:

AND (&)

OR (|)

isin()

isNull()

withColumn() for:

Creating new columns

Arithmetic operations

Constants using lit()

withColumnRenamed()

🔹 String Functions

regexp_replace() for data standardization

initcap()

upper()

lower()

🔹 Type Casting

Converting column data types using cast()

Handling numeric operations safely

🔹 Sorting & Limiting

orderBy() / sort()

Ascending & descending sort

Sorting with multiple columns

limit() function

🔹 Handling Duplicates

drop()

dropDuplicates()

distinct()

🔹 Union Operations

union()

unionByName()

Understanding schema alignment issues

🔹 Date Functions

current_date()

date_add()

date_sub()

datediff()

date_format()

🔹 Null Handling

dropna()

fillna()

Dropping nulls using subsets

🔹 Split & Indexing

split() function

Extracting values using getItem()

🔹 Group By & Aggregations

groupBy()

Aggregation functions:

sum()

avg()

Grouping with multiple columns

Aliasing aggregated columns

🎯 Learning Outcome

After completing this notebook, I gained:

Strong understanding of PySpark DataFrame API

Ability to clean, transform, and analyze large datasets

Practical experience with real-world data scenarios

Confidence to use PySpark for data engineering workflows

📌 How to Use

Clone the repository

Open the notebook in Databricks

Update file paths if needed

Run cells sequentially

📎 Author

Niruta Kadel
Data Enthusiast | PySpark Learner
