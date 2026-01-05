# 📊 PySpark DataFrame Operations – Advanced Tutorial Project

This repository contains a **complete end‑to‑end PySpark tutorial project** built by combining:

* 📘 **Advanced PySpark notebook / PDF concepts**
* 📓 **Hands‑on Databricks notebook implementation**
* 🧪 **Real CSV and JSON datasets**

The project demonstrates **beginner → advanced PySpark DataFrame operations**, making it ideal for **students, interns, and aspiring Data Engineers / Data Scientists**.

---

## 📌 Project Overview

The goal of this project is to provide **practical exposure to PySpark** using real‑world style data. It covers the full lifecycle of working with Spark DataFrames — from **data ingestion** to **advanced analytics using window functions and UDFs**.

All examples are executed in a **Databricks / Spark environment** using PySpark APIs.

---

## 🛠️ Technologies Used

* **Apache Spark (PySpark)**
* **Databricks Notebook**
* **Python**

---

## 📂 Data Sources

The following datasets are used throughout the project:

* **CSV File**: `BigMart Sales.csv`
* **JSON File**: `drivers.json`

### Example Databricks Volume Paths

```
/Volumes/workspace/pysparkcsv/csvfile/
/Volumes/workspace/pysparkcsv/sparkjson/
```

---

## 📖 Topics Covered

### 1️⃣ Data Reading

* Reading CSV files using `spark.read.format("csv")`
* Reading JSON files using `spark.read.format("json")`
* Options used:

  * `inferSchema`
  * `header`
  * `multiline`

---

### 2️⃣ Schema Definition

* Infer Schema
* DDL‑based schema
* `StructType` & `StructField` schema definition

---

### 3️⃣ Data Exploration

* `display()` for visualization (Databricks)
* `printSchema()`
* `select()` with column aliasing
* `collect()` to bring data to driver as Row objects

---

### 4️⃣ Filtering Data

* Filtering using column conditions
* Multiple conditions with `AND` / `OR`
* `isin()` usage
* `isNull()` and `isNotNull()`

---

### 5️⃣ Column Operations

* `withColumn()`
* `withColumnRenamed()`
* Creating constant columns using `lit()`
* Arithmetic column calculations

---

### 6️⃣ String Functions

* `initcap()`
* `upper()`
* `lower()`
* `regexp_replace()` for standardization

---

### 7️⃣ Type Casting

* Converting column data types using `cast()`

---

### 8️⃣ Sorting & Limiting

* `orderBy()` / `sort()`
* Sorting with multiple columns
* `limit()`

---

### 9️⃣ Handling Null Values

* `dropna()`
* `fillna()`
* Subset‑based null handling

---

### 🔟 Duplicate Handling

* `dropDuplicates()`
* `distinct()`

---

### 1️⃣1️⃣ Union Operations

* `union()`
* `unionByName()`

---

### 1️⃣2️⃣ Aggregations & Grouping

* `groupBy()` with:

  * `sum()`
  * `avg()`
* Multi‑column grouping
* Aliasing aggregated columns

---

### 1️⃣3️⃣ Pivot Operations

* Pivoting data using `pivot()`
* Aggregation after pivot (e.g., average Item_MRP by Outlet_Size)

---

### 1️⃣4️⃣ Conditional Logic (WHEN / OTHERWISE)

* Creating conditional columns using `when()` and `otherwise()`
* Nested conditions
* Real‑world example: veg / non‑veg classification and price‑based categories

---

### 1️⃣5️⃣ Joins in PySpark

Implemented join types:

* Inner Join
* Left Join
* Right Join
* Full Outer Join (conceptual)
* Left Semi Join (conceptual)
* Left Anti Join
* Cross Join (conceptual)

Practical examples using **employee and department datasets**.

---

### 1️⃣6️⃣ Window Functions

Window functions used without collapsing rows:

* `row_number()`
* `rank()`
* `dense_rank()`
* Cumulative Sum (`sum() over window`)

Window specifications include:

* `partitionBy()`
* `orderBy()`
* `rowsBetween()`

---

### 1️⃣7️⃣ User Defined Functions (UDF)

* Creating Python functions
* Registering them as PySpark UDFs
* Applying UDFs to DataFrame columns

Example: Squaring numeric column values using UDF

---

## 🚀 How to Run the Project

1. Upload `AdvancePyspark.ipynb` / `.py` to **Databricks** or Spark environment
2. Ensure datasets exist at configured paths
3. Execute cells sequentially to understand each operation

---

## 🎯 Learning Outcomes

After completing this project, you will be able to:

* Perform **advanced PySpark DataFrame operations**
* Work with schemas, joins, and window functions
* Handle real‑world data quality issues
* Apply conditional logic and aggregations
* Build strong foundations for **Big Data & Data Engineering roles**

---

## 📌 Future Enhancements

* Spark SQL queries
* Performance optimization & caching
* Broadcast joins
* PySpark unit testing

---

## 📘 Reference Material

This project is built by combining **Advanced PySpark learning material (PDF)** and **hands‑on Databricks notebook implementations**, focusing on industry‑relevant DataFrame processing techniques.

---

## 📄 License

This project is intended for **learning and educational purposes only**.

---

## ⭐ Support

If you find this repository helpful:

* ⭐ Star the repo
* 🍴 Fork it
* 📢 Share with other learners
