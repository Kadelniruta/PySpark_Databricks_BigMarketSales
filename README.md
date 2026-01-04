📊 PySpark DataFrame Operations – Documentation
📌 Project Overview

This repository contains my hands-on PySpark learning project, developed using Databricks Notebook.
The goal of this project is to understand and practice core PySpark DataFrame operations that are commonly used in data engineering and big data processing.

The notebook demonstrates how to read data, define schemas, clean data, transform columns, handle nulls, apply aggregations, and work with date & string functions using PySpark.

🛠️ Tech Stack

Apache Spark (PySpark)

Databricks

Python

CSV & JSON Files

📂 Dataset Information

JSON Dataset

Used to practice JSON reading and schema inference

CSV Dataset (BigMart Sales)

Used for schema definition, transformations, filtering, aggregations, and analysis

📥 Data Reading
JSON Data

Read JSON files using spark.read.format("json")

Used options:

inferSchema

header

multiline

CSV Data

Read CSV files using spark.read.format("csv")

Used options:

inferSchema

header

🧱 Schema Definition

This project demonstrates three ways of handling schema:

1️⃣ Auto-Inferred Schema

Spark automatically infers column data types

2️⃣ DDL Schema

Schema defined using SQL-style DDL

Useful for performance and data consistency

3️⃣ StructType Schema

Explicit schema using StructType and StructField

Recommended for production-level pipelines

🔍 DataFrame Operations
Select & Alias

select() specific columns

Rename columns using alias()

Filtering

Implemented multiple real-world filtering scenarios:

Filter by column value

Multiple conditions using &

isin() for multiple values

isNull() for null checks

🧩 Column Transformations
Creating New Columns

Used withColumn()

Created constant columns using lit()

Created derived columns using arithmetic expressions

Renaming Columns

Used withColumnRenamed()

🔤 String Functions

Applied string transformations using:

regexp_replace() (data standardization)

initcap()

upper()

lower()

🔄 Type Casting

Converted column data types using cast()

Ensured correct numeric operations on string columns

🔃 Sorting & Limiting

Sorted data using:

orderBy()

sort()

Used:

Ascending and descending order

Multiple-column sorting

Limited rows using limit()

🧹 Data Cleaning
Dropping Columns

drop() single or multiple columns

Handling Duplicates

dropDuplicates()

distinct()

Handling Null Values

dropna() to remove null rows

fillna() to replace nulls

Subset-based null handling

🔗 Union Operations

union() → requires same column order

unionByName() → matches columns by name

📅 Date Functions

Worked with Spark date functions:

current_date()

date_add() and date_sub()

datediff()

date_format()

🧠 Split & Indexing

Split string columns using split()

Extracted values using getItem()

📊 Group By & Aggregations

Performed aggregations using:

groupBy()

sum()

avg()

Grouped by:

Single column

Multiple columns

Used aliases for aggregated columns

🎯 Learning Outcomes

Through this project, I gained:

Strong understanding of PySpark DataFrame API

Hands-on experience with data transformation and cleaning

Knowledge of schema management techniques

Confidence to work on data engineering tasks using Spark

▶️ How to Run

Clone this repository

Upload the notebook to Databricks

Update file paths if required

Run cells sequentially

👤 Author

Niruta Kadel
Data Enthusiast | PySpark Learner
