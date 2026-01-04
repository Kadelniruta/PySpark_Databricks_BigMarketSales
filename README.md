# PySpark DataFrame Operations – Tutorial Project

This project is a **hands-on PySpark tutorial** demonstrating commonly used **DataFrame operations** such as reading data, schema definition, filtering, transformations, aggregations, and date/string functions. The notebook is designed for learners who want practical exposure to PySpark using real datasets.

---

## 📌 Project Overview

The notebook covers end-to-end PySpark DataFrame usage including:

* Reading **JSON** and **CSV** files
* Defining schemas using multiple approaches
* Data selection, filtering, and transformation
* Handling nulls and duplicates
* Sorting, grouping, and aggregations
* Working with strings and dates

The examples are executed in a **Databricks / Spark environment** using PySpark.

---

## 🛠️ Technologies Used

* **Apache Spark (PySpark)**
* **Databricks Notebook**
* **Python**

---

## 📂 Data Sources

The following datasets are used:

* **JSON File**: `drivers.json`
* **CSV File**: `BigMart Sales.csv`

Paths (Databricks volume example):

```
/Volumes/workspace/pysparkcsv/sparkjson/
/Volumes/workspace/pysparkcsv/csvfile/
```

---

## 📖 Topics Covered

### 1. Data Reading

* Reading JSON files using `spark.read.format('json')`
* Reading CSV files using `spark.read.format('csv')`
* Options such as `inferSchema`, `header`, and `multiline`

---

### 2. Schema Definition

Different schema definition techniques:

* **Infer Schema**
* **DDL Schema**
* **StructType & StructField Schema**

---

### 3. Data Exploration

* `display()`
* `printSchema()`
* `select()` and column aliasing

---

### 4. Filtering Data

Examples include:

* Filtering by column values
* Multiple condition filters
* `isin()` and `isNull()` usage

---

### 5. Column Operations

* `withColumn()`
* `withColumnRenamed()`
* Creating constant columns using `lit()`
* Column calculations

---

### 6. String Functions

* `initcap()`
* `upper()`
* `lower()`
* `regexp_replace()` for data standardization

---

### 7. Type Casting

* Converting column data types using `cast()`

---

### 8. Sorting & Limiting

* `orderBy()` / `sort()`
* Sorting with multiple columns
* `limit()`

---

### 9. Handling Nulls

* Dropping null values using `dropna()`
* Filling null values using `fillna()`
* Subset-based null handling

---

### 10. Duplicate Handling

* `dropDuplicates()`
* `distinct()`

---

### 11. Union Operations

* `union()`
* `unionByName()`

---

### 12. Date Functions

* `current_date()`
* `date_add()` and `date_sub()`
* `datediff()`
* `date_format()`

---

### 13. Group By & Aggregations

* `groupBy()` with:

  * `sum()`
  * `avg()`
* Multi-column groupings
* Aliasing aggregated columns

---

## 🚀 How to Run

1. Upload the notebook to **Databricks** or run in a **Spark-enabled environment**
2. Ensure datasets are available at the configured paths
3. Execute cells sequentially to understand each operation

---

## 🎯 Learning Outcome

After completing this tutorial, you will be able to:

* Perform real-world PySpark DataFrame operations
* Apply transformations and aggregations efficiently
* Handle missing data and schema definitions
* Use PySpark functions for string and date processing

---

## 📌 Future Enhancements

* Add joins and window functions
* Include performance optimization examples
* Add unit tests using PySpark testing frameworks

---

## 📄 License

This project is intended for **learning and educational purposes**.

---

### ⭐ If you find this helpful, feel free to star the repository!
